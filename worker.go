// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sessionflight

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"
)

// Run consumes and processes session events until ctx is cancelled, then
// waits for in-flight runs to unwind before returning. Handlers receive a
// context derived from ctx, so cancelling ctx interrupts long runs;
// interrupted sessions stay pending in Valkey and are retried later.
func (r *Runner) Run(ctx context.Context) error {
	if r.cfg.Handler == nil {
		return errors.New("sessionflight: Config.Handler is required to call Run")
	}

	sem := make(chan struct{}, r.cfg.Concurrency)
	var wg sync.WaitGroup
	inflight := &inflightSet{ids: map[string]struct{}{}, bySID: map[string][]string{}}
	var lastClaim time.Time

	for ctx.Err() == nil {
		free := r.cfg.Concurrency - inflight.len()
		if free <= 0 {
			// All slots busy; wait a moment rather than reading entries we
			// cannot start (an unstarted entry has no heartbeat and could
			// be stolen by another runner's autoclaim).
			select {
			case <-ctx.Done():
			case <-time.After(r.cfg.LockRetryInterval):
			}
			continue
		}

		var entries []valkey.XRangeEntry
		if time.Since(lastClaim) >= r.cfg.AutoclaimInterval {
			lastClaim = time.Now()
			entries = r.autoclaim(ctx, int64(free))
		}
		if len(entries) == 0 {
			entries = r.readNew(ctx, int64(free))
		}

		for _, e := range entries {
			sid := e.FieldValues["sid"]
			if !inflight.add(sid, e.ID) {
				continue // already being processed by this runner
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				inflight.remove(sid, e.ID)
				continue
			}
			wg.Add(1)
			go func(e valkey.XRangeEntry, sid string) {
				defer wg.Done()
				defer func() { <-sem }()
				defer inflight.remove(sid, e.ID)
				r.process(ctx, e, inflight)
			}(e, sid)
		}
	}

	wg.Wait()
	return ctx.Err()
}

// readNew blocks up to BlockTimeout for new (never-delivered) entries.
func (r *Runner) readNew(ctx context.Context, count int64) []valkey.XRangeEntry {
	c := r.cfg.Client
	res := c.Do(ctx, c.B().Xreadgroup().Group(r.cfg.Group, r.cfg.Consumer).
		Count(count).Block(r.cfg.BlockTimeout.Milliseconds()).
		Streams().Key(r.stream).Id(">").Build())
	if err := res.Error(); err != nil {
		if !valkey.IsValkeyNil(err) && ctx.Err() == nil {
			r.cfg.Logger.Error("sessionflight: XREADGROUP failed", "err", err)
			r.sleep(ctx, time.Second)
		}
		return nil
	}
	read, err := res.AsXRead()
	if err != nil {
		r.cfg.Logger.Error("sessionflight: parsing XREADGROUP reply", "err", err)
		return nil
	}
	return read[r.stream]
}

// autoclaim steals pending entries whose consumer stopped heartbeating —
// crashed runners, or readers that died between delivery and processing.
func (r *Runner) autoclaim(ctx context.Context, count int64) []valkey.XRangeEntry {
	c := r.cfg.Client
	res := c.Do(ctx, c.B().Xautoclaim().Key(r.stream).Group(r.cfg.Group).
		Consumer(r.cfg.Consumer).
		MinIdleTime(strconv.FormatInt(r.cfg.ClaimMinIdle.Milliseconds(), 10)).
		Start("0-0").Count(count).Build())
	if err := res.Error(); err != nil {
		if ctx.Err() == nil {
			r.cfg.Logger.Error("sessionflight: XAUTOCLAIM failed", "err", err)
		}
		return nil
	}
	arr, err := res.ToArray()
	if err != nil || len(arr) < 2 {
		r.cfg.Logger.Error("sessionflight: unexpected XAUTOCLAIM reply", "err", err)
		return nil
	}
	entries, err := arr[1].AsXRange()
	if err != nil {
		r.cfg.Logger.Error("sessionflight: parsing XAUTOCLAIM entries", "err", err)
		return nil
	}
	return entries
}

// process drives one delivery of one entry through the full lifecycle:
// attempt accounting, per-session lock, handler, cleanup, ack.
func (r *Runner) process(ctx context.Context, e valkey.XRangeEntry, inflight *inflightSet) {
	log := r.cfg.Logger

	ev, err := parseEvent(e)
	if err != nil {
		// Malformed entry: nothing to run; ack it away so it cannot wedge
		// the group, but keep a copy in the dead-letter stream.
		log.Error("sessionflight: malformed entry, dead-lettering", "entry", e.ID, "err", err)
		r.deadLetter(ctx, &Event{SessionID: "?", EntryID: e.ID}, "malformed: "+err.Error())
		r.ack(context.WithoutCancel(ctx), e.ID)
		return
	}

	ev.Attempt = r.deliveryCount(ctx, e.ID)

	// runCtx is what the handler sees. Besides shutdown, it is cancelled by
	// the heartbeat if the session lock is lost (e.g. the process stalled
	// past LockTTL and another run took over), so at most one handler runs
	// per session ID as long as handlers honor their context.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	// Heartbeat for the whole time this entry is ours — including while we
	// wait for the session lock — so no other runner autoclaims it.
	token := r.cfg.Consumer + "/" + e.ID
	var lockHeld atomic.Bool
	hbCtx, stopHB := context.WithCancel(context.WithoutCancel(ctx))
	defer stopHB()
	go r.heartbeat(hbCtx, ev, token, &lockHeld, cancelRun)

	// Preserve enqueue order for entries of the same session ID that were
	// delivered to this runner: only the oldest may contend for the lock.
	// Back-to-back enqueues usually land in the same delivery batch, so
	// this keeps them FIFO instead of racing for the lock.
	for !inflight.isOldestForSID(ev.SessionID, e.ID) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.cfg.LockRetryInterval):
		}
	}

	// Serialize runs of the same session ID across runners.
	if err := r.acquireLock(ctx, ev.SessionID, token); err != nil {
		if ctx.Err() == nil {
			log.Error("sessionflight: acquiring session lock", "session", ev.SessionID, "err", err)
		}
		return // stays pending; retried after ClaimMinIdle
	}
	lockHeld.Store(true)
	defer r.releaseLock(context.WithoutCancel(ctx), ev.SessionID, token)

	// Dead-letter under the lock so the cleanup hook cannot overlap with a
	// live run of the same session.
	if ev.Attempt > r.cfg.MaxDeliveries {
		log.Warn("sessionflight: event exceeded max deliveries, dead-lettering",
			"session", ev.SessionID, "entry", ev.EntryID, "attempts", ev.Attempt)
		r.deadLetter(ctx, ev, ErrDeadLettered.Error())
		r.runCleanup(ev, ErrDeadLettered)
		r.ack(context.WithoutCancel(ctx), e.ID)
		return
	}

	// Skip the handler if a previous attempt already completed it but
	// crashed before acking.
	done, err := r.doneMarkerExists(ctx, e.ID)
	if err != nil {
		log.Error("sessionflight: checking done marker", "entry", e.ID, "err", err)
		return
	}
	ev.HandlerSkipped = done

	var runErr error
	if !done {
		runErr = r.runHandler(runCtx, ev)
		if runErr == nil {
			// Record success before acking so a crash in between does not
			// re-run the handler. Best effort: if this write is lost, the
			// worst case is one extra handler run.
			if err := r.setDoneMarker(context.WithoutCancel(ctx), e.ID); err != nil {
				log.Warn("sessionflight: writing done marker", "entry", e.ID, "err", err)
			}
		}
	}

	if runErr != nil {
		// No cleanup for retryable failures: the run has not concluded.
		// Cleanup fires when it eventually succeeds or is dead-lettered.
		log.Warn("sessionflight: session run failed, will retry",
			"session", ev.SessionID, "entry", ev.EntryID, "attempt", ev.Attempt, "err", runErr)
		return // no ack: redelivered after ClaimMinIdle
	}

	r.runCleanup(ev, nil)

	ackCtx := context.WithoutCancel(ctx)
	r.ack(ackCtx, e.ID)
	r.clearDoneMarker(ackCtx, e.ID)
}

// runHandler invokes the user handler, converting panics into errors so a
// panicking session is retried (and eventually dead-lettered) instead of
// killing the runner.
func (r *Runner) runHandler(ctx context.Context, ev *Event) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("sessionflight: handler panicked: %v", rec)
		}
	}()
	return r.cfg.Handler(ctx, ev)
}

// runCleanup invokes the user cleanup hook on a fresh bounded context so it
// executes even during shutdown, and never lets a panic escape.
func (r *Runner) runCleanup(ev *Event, runErr error) {
	if r.cfg.Cleanup == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.CleanupTimeout)
	defer cancel()
	defer func() {
		if rec := recover(); rec != nil {
			r.cfg.Logger.Error("sessionflight: cleanup panicked",
				"session", ev.SessionID, "entry", ev.EntryID, "panic", rec)
		}
	}()
	r.cfg.Cleanup(ctx, ev, runErr)
}

// heartbeat keeps this delivery visibly alive: it resets the entry's idle
// time in the pending list (so autoclaim skips it) and renews the session
// lock once held. If the lock is definitively lost — or renewals keep
// failing for so long that it must have expired — it calls cancelRun to
// stop the handler: another run of this session may have started, and the
// single-run-per-session guarantee takes priority over finishing this one.
func (r *Runner) heartbeat(ctx context.Context, ev *Event, token string, lockHeld *atomic.Bool, cancelRun context.CancelFunc) {
	c := r.cfg.Client
	t := time.NewTicker(r.cfg.HeartbeatInterval)
	defer t.Stop()
	lastRenewed := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		// XCLAIM JUSTID to ourselves resets the idle clock without
		// bumping the delivery counter.
		err := c.Do(ctx, c.B().Xclaim().Key(r.stream).Group(r.cfg.Group).
			Consumer(r.cfg.Consumer).MinIdleTime("0").Id(ev.EntryID).Justid().Build()).Error()
		if err != nil && ctx.Err() == nil {
			r.cfg.Logger.Warn("sessionflight: heartbeat XCLAIM failed",
				"entry", ev.EntryID, "err", err)
		}

		if !lockHeld.Load() {
			lastRenewed = time.Now()
			continue
		}
		switch err := r.renewLock(ctx, ev.SessionID, token); {
		case err == nil:
			lastRenewed = time.Now()
		case ctx.Err() != nil:
			return
		case errors.Is(err, errLockLost):
			r.cfg.Logger.Error("sessionflight: session lock lost, cancelling run",
				"session", ev.SessionID, "entry", ev.EntryID)
			cancelRun()
			return
		default:
			r.cfg.Logger.Warn("sessionflight: heartbeat lock renewal failed",
				"session", ev.SessionID, "err", err)
			if time.Since(lastRenewed) > r.cfg.LockTTL {
				r.cfg.Logger.Error("sessionflight: lock not renewed within LockTTL, cancelling run",
					"session", ev.SessionID, "entry", ev.EntryID)
				cancelRun()
				return
			}
		}
	}
}

// deliveryCount reads how many times this entry has been delivered.
func (r *Runner) deliveryCount(ctx context.Context, entryID string) int64 {
	c := r.cfg.Client
	res := c.Do(ctx, c.B().Xpending().Key(r.stream).Group(r.cfg.Group).
		Start(entryID).End(entryID).Count(1).Build())
	arr, err := res.ToArray()
	if err != nil || len(arr) == 0 {
		return 1
	}
	item, err := arr[0].ToArray()
	if err != nil || len(item) < 4 {
		return 1
	}
	n, err := item[3].AsInt64()
	if err != nil || n < 1 {
		return 1
	}
	return n
}

func (r *Runner) doneMarkerExists(ctx context.Context, entryID string) (bool, error) {
	c := r.cfg.Client
	n, err := c.Do(ctx, c.B().Exists().Key(r.doneKey(entryID)).Build()).AsInt64()
	return n > 0, err
}

func (r *Runner) setDoneMarker(ctx context.Context, entryID string) error {
	c := r.cfg.Client
	return c.Do(ctx, c.B().Set().Key(r.doneKey(entryID)).Value("1").
		Px(r.cfg.DoneMarkerTTL).Build()).Error()
}

func (r *Runner) clearDoneMarker(ctx context.Context, entryID string) {
	c := r.cfg.Client
	if err := c.Do(ctx, c.B().Del().Key(r.doneKey(entryID)).Build()).Error(); err != nil {
		r.cfg.Logger.Warn("sessionflight: clearing done marker", "entry", entryID, "err", err)
	}
}

// ack acknowledges the entry and deletes it from the stream so the stream
// does not grow without bound.
func (r *Runner) ack(ctx context.Context, entryID string) {
	c := r.cfg.Client
	if err := c.Do(ctx, c.B().Xack().Key(r.stream).Group(r.cfg.Group).
		Id(entryID).Build()).Error(); err != nil {
		r.cfg.Logger.Error("sessionflight: XACK failed", "entry", entryID, "err", err)
		return
	}
	if err := c.Do(ctx, c.B().Xdel().Key(r.stream).Id(entryID).Build()).Error(); err != nil {
		r.cfg.Logger.Warn("sessionflight: XDEL failed", "entry", entryID, "err", err)
	}
}

// deadLetter copies the entry to the dead-letter stream.
func (r *Runner) deadLetter(ctx context.Context, ev *Event, reason string) {
	c := r.cfg.Client
	err := c.Do(context.WithoutCancel(ctx), c.B().Xadd().Key(r.dead).Id("*").
		FieldValue().
		FieldValue("sid", ev.SessionID).
		FieldValue("entry_id", ev.EntryID).
		FieldValue("attempts", strconv.FormatInt(ev.Attempt, 10)).
		FieldValue("reason", reason).
		Build()).Error()
	if err != nil {
		r.cfg.Logger.Error("sessionflight: writing dead-letter entry",
			"session", ev.SessionID, "entry", ev.EntryID, "err", err)
	}
}

func (r *Runner) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// inflightSet tracks entries currently being processed by this runner so
// the read loop never starts the same entry twice, and orders in-flight
// entries per session ID so same-session runs execute in enqueue order.
type inflightSet struct {
	mu    sync.Mutex
	ids   map[string]struct{}
	bySID map[string][]string
}

func (s *inflightSet) add(sid, id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ids[id]; ok {
		return false
	}
	s.ids[id] = struct{}{}
	if sid != "" {
		s.bySID[sid] = append(s.bySID[sid], id)
	}
	return true
}

func (s *inflightSet) remove(sid, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.ids, id)
	if sid == "" {
		return
	}
	q := s.bySID[sid]
	for i, v := range q {
		if v == id {
			q = append(q[:i], q[i+1:]...)
			break
		}
	}
	if len(q) == 0 {
		delete(s.bySID, sid)
	} else {
		s.bySID[sid] = q
	}
}

func (s *inflightSet) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ids)
}

// isOldestForSID reports whether id is the oldest (lowest stream entry ID)
// in-flight entry for the session on this runner.
func (s *inflightSet) isOldestForSID(sid, id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, other := range s.bySID[sid] {
		if other != id && entryIDLess(other, id) {
			return false
		}
	}
	return true
}

// entryIDLess compares stream entry IDs ("<ms>-<seq>") numerically.
func entryIDLess(a, b string) bool {
	ams, aseq, _ := strings.Cut(a, "-")
	bms, bseq, _ := strings.Cut(b, "-")
	am, _ := strconv.ParseInt(ams, 10, 64)
	bm, _ := strconv.ParseInt(bms, 10, 64)
	if am != bm {
		return am < bm
	}
	as, _ := strconv.ParseInt(aseq, 10, 64)
	bs, _ := strconv.ParseInt(bseq, 10, 64)
	return as < bs
}
