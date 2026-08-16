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

// Package sessionflight orchestrates long-running "session" jobs on top of
// Valkey streams.
//
// Session run requests (events) are enqueued with a logical session ID.
// Runners consume them through a Valkey consumer group,
// run a user-supplied Handler, invoke a Cleanup hook once the run concludes
// (success or dead-letter), and acknowledge the event only once the run and
// cleanup have finished.
//
// Resilience properties:
//
//   - A runner that crashes mid-run never loses the event: the stream
//     entry stays in the consumer group's pending list and is reclaimed by
//     another runner (XAUTOCLAIM) once its idle time exceeds ClaimMinIdle.
//   - A live run heartbeats (XCLAIM JUSTID) so its entry never looks idle
//     and cannot be stolen while the handler is still working.
//   - At most one handler runs per session ID at any time: a per-session-ID
//     lock serializes runs, even across runner processes. If a run's lock
//     is ever lost (the process stalled past LockTTL, so another run may
//     have started), the handler's context is cancelled — so the guarantee
//     holds as long as handlers honor their context. Events
//     for the same session ID delivered to the same runner execute in
//     enqueue order; across runners and across retries ordering is
//     best-effort.
//   - If a runner crashes after the handler succeeded but before the
//     acknowledgment, a completion marker prevents the handler from running
//     twice: the redelivery only performs cleanup and the ack.
//   - Events that keep failing are moved to a dead-letter stream after
//     MaxDeliveries attempts instead of looping forever.
package sessionflight

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// ErrDeadLettered is passed to Cleanup when an event exhausted
// MaxDeliveries attempts and was moved to the dead-letter stream.
var ErrDeadLettered = errors.New("sessionflight: max deliveries exceeded, event dead-lettered")

// Event describes one session run request delivered to the Handler and
// Cleanup.
type Event struct {
	// SessionID is the logical session ID supplied to Enqueue. Multiple
	// stream entries may share the same session ID; their runs are
	// serialized.
	SessionID string
	// EntryID is the unique Valkey stream entry ID of this run request.
	EntryID string
	// EnqueuedAt is derived from the stream entry ID.
	EnqueuedAt time.Time
	// Attempt is the delivery attempt number, starting at 1.
	Attempt int64
	// HandlerSkipped is true when a previous attempt already completed the
	// handler successfully (the runner crashed before acknowledging); only
	// cleanup and the ack are performed on this attempt.
	HandlerSkipped bool
}

// Handler runs one session. It may take a long time; the runner heartbeats
// on its behalf while it runs. Returning nil acknowledges the event;
// returning an error leaves it pending for a later retry.
//
// The context is cancelled on shutdown (Run's context cancelled) and when
// the session lock is lost. Handlers must honor the cancellation and stop:
// that is what upholds the at-most-one-run-per-session guarantee.
type Handler func(ctx context.Context, e *Event) error

// Cleanup is invoked when an event's run concludes: after the handler
// succeeds (runErr == nil) or when the event is dead-lettered (runErr is
// ErrDeadLettered). Failed attempts that will be retried do not trigger
// cleanup. It runs on a fresh context with CleanupTimeout so it still
// executes during shutdown. It may run more than once for the same event
// after a crash, so make it idempotent.
type Cleanup func(ctx context.Context, e *Event, runErr error)

// Config configures a Runner. Client and Handler are required to run;
// only Client is required if the process just enqueues.
type Config struct {
	// Client is a connected valkey-go client. The runner does not close it.
	Client valkey.Client

	// Prefix namespaces every key this library touches.
	// Default "sessionflight".
	Prefix string
	// Group is the consumer group name. Default "workers".
	Group string
	// Consumer is this runner's consumer name. It should be unique per
	// live process. Default "<hostname>-<pid>".
	Consumer string

	// Handler runs sessions. Required to call Run.
	Handler Handler
	// Cleanup, if set, runs when an event's run concludes (success or
	// dead-letter). Optional.
	Cleanup Cleanup

	// Concurrency is the number of sessions this runner runs at once.
	// Default 8.
	Concurrency int
	// BlockTimeout is how long one XREADGROUP call blocks waiting for new
	// events. Default 5s.
	BlockTimeout time.Duration
	// HeartbeatInterval is how often a live run refreshes its claim on the
	// stream entry and its session lock. Default 10s.
	HeartbeatInterval time.Duration
	// ClaimMinIdle is how long a pending entry must sit without a
	// heartbeat before another runner may steal it. Must be at least
	// 3*HeartbeatInterval. Default 60s.
	ClaimMinIdle time.Duration
	// AutoclaimInterval is how often the runner scans for stale pending
	// entries. Default 10s.
	AutoclaimInterval time.Duration
	// LockTTL is the per-session-ID lock TTL; the heartbeat renews it.
	// Must be at least 3*HeartbeatInterval. Default 60s.
	LockTTL time.Duration
	// LockRetryInterval is the poll interval while waiting for another run
	// of the same session ID to finish. Default 250ms.
	LockRetryInterval time.Duration
	// MaxDeliveries dead-letters an entry once its delivery count exceeds
	// this. Default 3.
	MaxDeliveries int64
	// DoneMarkerTTL bounds how long "handler already succeeded" markers
	// live if the ack path is never reached again. Default 24h.
	DoneMarkerTTL time.Duration
	// CleanupTimeout bounds each Cleanup invocation. Default 30s.
	CleanupTimeout time.Duration

	// Logger receives operational errors (heartbeat failures, parse
	// errors, ...). Default slog.Default().
	Logger *slog.Logger
}

// Runner enqueues session events and runs them.
type Runner struct {
	cfg    Config
	stream string
	dead   string
}

// New validates cfg, applies defaults, and ensures the stream and consumer
// group exist.
func New(ctx context.Context, cfg Config) (*Runner, error) {
	if cfg.Client == nil {
		return nil, errors.New("sessionflight: Config.Client is required")
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "sessionflight"
	}
	if cfg.Group == "" {
		cfg.Group = "workers"
	}
	if cfg.Consumer == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "runner"
		}
		cfg.Consumer = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 8
	}
	if cfg.BlockTimeout <= 0 {
		cfg.BlockTimeout = 5 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}
	if cfg.ClaimMinIdle <= 0 {
		cfg.ClaimMinIdle = 60 * time.Second
	}
	if cfg.AutoclaimInterval <= 0 {
		cfg.AutoclaimInterval = 10 * time.Second
	}
	if cfg.LockTTL <= 0 {
		cfg.LockTTL = 60 * time.Second
	}
	if cfg.LockRetryInterval <= 0 {
		cfg.LockRetryInterval = 250 * time.Millisecond
	}
	if cfg.MaxDeliveries <= 0 {
		cfg.MaxDeliveries = 3
	}
	if cfg.DoneMarkerTTL <= 0 {
		cfg.DoneMarkerTTL = 24 * time.Hour
	}
	if cfg.CleanupTimeout <= 0 {
		cfg.CleanupTimeout = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ClaimMinIdle < 3*cfg.HeartbeatInterval {
		return nil, fmt.Errorf("sessionflight: ClaimMinIdle (%v) must be at least 3*HeartbeatInterval (%v)",
			cfg.ClaimMinIdle, 3*cfg.HeartbeatInterval)
	}
	if cfg.LockTTL < 3*cfg.HeartbeatInterval {
		return nil, fmt.Errorf("sessionflight: LockTTL (%v) must be at least 3*HeartbeatInterval (%v)",
			cfg.LockTTL, 3*cfg.HeartbeatInterval)
	}

	r := &Runner{
		cfg:    cfg,
		stream: cfg.Prefix + ":stream",
		dead:   cfg.Prefix + ":dead",
	}

	err := cfg.Client.Do(ctx, cfg.Client.B().XgroupCreate().
		Key(r.stream).Group(cfg.Group).Id("0").Mkstream().Build()).Error()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return nil, fmt.Errorf("sessionflight: creating consumer group: %w", err)
	}
	return r, nil
}

// Enqueue submits a new run request for sessionID and returns the stream
// entry ID. Enqueuing the same sessionID again is allowed; the runs never
// overlap and execute in enqueue order when delivered to the same runner.
func (r *Runner) Enqueue(ctx context.Context, sessionID string) (string, error) {
	if sessionID == "" {
		return "", errors.New("sessionflight: sessionID must not be empty")
	}
	c := r.cfg.Client
	res := c.Do(ctx, c.B().Xadd().Key(r.stream).Id("*").
		FieldValue().
		FieldValue("sid", sessionID).
		Build())
	entryID, err := res.ToString()
	if err != nil {
		return "", fmt.Errorf("sessionflight: enqueue: %w", err)
	}
	return entryID, nil
}

// DeadLetterStream returns the key of the dead-letter stream. Entries have
// fields sid, entry_id, attempts, reason.
func (r *Runner) DeadLetterStream() string { return r.dead }

// Stream returns the key of the main session stream.
func (r *Runner) Stream() string { return r.stream }

func (r *Runner) lockKey(sessionID string) string {
	return r.cfg.Prefix + ":lock:" + sessionID
}

func (r *Runner) doneKey(entryID string) string {
	return r.cfg.Prefix + ":done:" + entryID
}

func parseEvent(e valkey.XRangeEntry) (*Event, error) {
	sid := e.FieldValues["sid"]
	if sid == "" {
		return nil, fmt.Errorf("entry %s has no sid field", e.ID)
	}
	ev := &Event{
		SessionID: sid,
		EntryID:   e.ID,
	}
	if ms, _, ok := strings.Cut(e.ID, "-"); ok {
		if n, err := strconv.ParseInt(ms, 10, 64); err == nil {
			ev.EnqueuedAt = time.UnixMilli(n)
		}
	}
	return ev, nil
}
