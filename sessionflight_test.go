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

// Tests run against an in-process miniredis by default. Set VALKEY_ADDR to
// run them against a real Valkey server instead.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/valkey-io/valkey-go"
)

var valkeyAddr string

func TestMain(m *testing.M) {
	if addr := os.Getenv("VALKEY_ADDR"); addr != "" {
		valkeyAddr = addr
		os.Exit(m.Run())
	}
	mini, err := miniredis.Run()
	if err != nil {
		fmt.Printf("starting miniredis: %v\n", err)
		os.Exit(1)
	}
	valkeyAddr = mini.Addr()
	code := m.Run()
	mini.Close()
	os.Exit(code)
}

func newTestClient(t *testing.T) valkey.Client {
	t.Helper()
	c, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{valkeyAddr},
		// miniredis does not support client-side caching (CLIENT TRACKING).
		DisableCache: true,
	})
	if err != nil {
		t.Fatalf("connecting to valkey at %s: %v", valkeyAddr, err)
	}
	t.Cleanup(c.Close)
	return c
}

var testRunSeq atomic.Int64

// fastCfg returns a Config with aggressive timings suitable for tests.
// Each test invocation gets its own key prefix (unique even under
// -count=N) so tests are isolated.
func fastCfg(t *testing.T, c valkey.Client) Config {
	return Config{
		Client:            c,
		Prefix:            fmt.Sprintf("t%d:%s", testRunSeq.Add(1), t.Name()),
		Group:             "workers",
		Consumer:          "test-runner",
		Concurrency:       4,
		BlockTimeout:      200 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		ClaimMinIdle:      400 * time.Millisecond,
		AutoclaimInterval: 100 * time.Millisecond,
		LockTTL:           400 * time.Millisecond,
		LockRetryInterval: 20 * time.Millisecond,
		MaxDeliveries:     3,
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestRunAckAndCleanup(t *testing.T) {
	c := newTestClient(t)
	cfg := fastCfg(t, c)

	var ran, cleaned atomic.Int32
	cfg.Handler = func(ctx context.Context, e *Event) error {
		if e.SessionID != "s1" {
			t.Errorf("SessionID = %q, want %q", e.SessionID, "s1")
		}
		ran.Add(1)
		return nil
	}
	cfg.Cleanup = func(ctx context.Context, e *Event, runErr error) {
		if runErr != nil {
			t.Errorf("cleanup got unexpected error: %v", runErr)
		}
		cleaned.Add(1)
	}

	r, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Enqueue(t.Context(), "s1"); err != nil {
		t.Fatal(err)
	}

	go r.Run(t.Context())

	waitFor(t, 5*time.Second, "run and cleanup", func() bool {
		return ran.Load() == 1 && cleaned.Load() == 1
	})

	// The entry must be acked (no pending) and deleted from the stream.
	waitFor(t, 5*time.Second, "ack", func() bool {
		return pendingCount(t, c, r) == 0 && streamLen(t, c, r.Stream()) == 0
	})
}

func TestSameSessionIDSerialized(t *testing.T) {
	c := newTestClient(t)
	cfg := fastCfg(t, c)

	var mu sync.Mutex
	var running int
	var maxRunning int
	var order []string
	var done atomic.Int32
	cfg.Handler = func(ctx context.Context, e *Event) error {
		mu.Lock()
		running++
		if running > maxRunning {
			maxRunning = running
		}
		order = append(order, e.EntryID)
		mu.Unlock()

		time.Sleep(300 * time.Millisecond) // long enough that runs would overlap if unserialized

		mu.Lock()
		running--
		mu.Unlock()
		done.Add(1)
		return nil
	}

	r, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	id1, _ := r.Enqueue(t.Context(), "same-session")
	id2, _ := r.Enqueue(t.Context(), "same-session")

	go r.Run(t.Context())

	waitFor(t, 10*time.Second, "both runs to finish", func() bool { return done.Load() == 2 })

	mu.Lock()
	defer mu.Unlock()
	if maxRunning != 1 {
		t.Errorf("runs for the same session_id overlapped: max concurrent = %d", maxRunning)
	}
	if len(order) != 2 || order[0] != id1 || order[1] != id2 {
		t.Errorf("run order = %v, want [%s %s]", order, id1, id2)
	}
}

// TestConcurrentEnqueuesRunOneByOne enqueues the same session ID from 5
// concurrent goroutines and verifies the runs execute strictly one at a
// time, in stream (enqueue) order, with none lost.
func TestConcurrentEnqueuesRunOneByOne(t *testing.T) {
	c := newTestClient(t)
	cfg := fastCfg(t, c)
	const enqueuers = 5
	cfg.Concurrency = 2 * enqueuers // all events in flight at once; the lock must do the serializing

	var mu sync.Mutex
	var running, maxRunning int
	var got []string // entry IDs in execution order
	var done atomic.Int32
	cfg.Handler = func(ctx context.Context, e *Event) error {
		mu.Lock()
		running++
		if running > maxRunning {
			maxRunning = running
		}
		got = append(got, e.EntryID)
		mu.Unlock()

		time.Sleep(100 * time.Millisecond) // runs would overlap here if unserialized

		mu.Lock()
		running--
		mu.Unlock()
		done.Add(1)
		return nil
	}

	r, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	// 5 goroutines race to enqueue the same session ID.
	ids := make([]string, enqueuers)
	var wg sync.WaitGroup
	for i := range enqueuers {
		wg.Go(func() {
			id, err := r.Enqueue(t.Context(), "one-by-one")
			if err != nil {
				t.Errorf("enqueue: %v", err)
				return
			}
			ids[i] = id
		})
	}
	wg.Wait()

	go r.Run(t.Context())

	waitFor(t, 1*time.Second, "all 5 runs to finish", func() bool {
		return done.Load() == enqueuers && pendingCount(t, c, r) == 0
	})

	mu.Lock()
	defer mu.Unlock()
	if maxRunning != 1 {
		t.Errorf("runs for the same session_id overlapped: max concurrent = %d, want 1", maxRunning)
	}
	if len(got) != enqueuers {
		t.Fatalf("executed %d runs, want %d (order: %v)", len(got), enqueuers, got)
	}
	// The stream assigns monotonically increasing entry IDs, so enqueue
	// order is the ascending entry-ID order; execution must follow it.
	want := append([]string(nil), ids...)
	sort.Slice(want, func(i, j int) bool { return entryIDLess(want[i], want[j]) })
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("execution order = %v, want %v", got, want)
		}
	}
}

// TestMutualExclusionAcrossRunners hammers the core guarantee: many events
// for one session ID, consumed by two competing runner processes, must
// never produce two handlers running at once.
func TestMutualExclusionAcrossRunners(t *testing.T) {
	c := newTestClient(t)
	cfg := fastCfg(t, c)

	const events = 6
	var mu sync.Mutex
	var running, maxRunning int
	var done atomic.Int32
	handler := func(ctx context.Context, e *Event) error {
		mu.Lock()
		running++
		if running > maxRunning {
			maxRunning = running
		}
		mu.Unlock()
		time.Sleep(80 * time.Millisecond)
		mu.Lock()
		running--
		mu.Unlock()
		done.Add(1)
		return nil
	}

	cfg.Handler = handler
	r1, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg2 := cfg
	cfg2.Consumer = "second-runner"
	r2, err := New(t.Context(), cfg2)
	if err != nil {
		t.Fatal(err)
	}

	for range events {
		if _, err := r1.Enqueue(t.Context(), "contended"); err != nil {
			t.Fatal(err)
		}
	}

	go r1.Run(t.Context())
	go r2.Run(t.Context())

	waitFor(t, 20*time.Second, "all runs to finish", func() bool {
		return done.Load() == events && pendingCount(t, c, r1) == 0
	})
	mu.Lock()
	defer mu.Unlock()
	if maxRunning != 1 {
		t.Errorf("max concurrent runs for one session = %d, want 1", maxRunning)
	}
}

// TestLockLossCancelsRun verifies the fencing behavior: if a run's session
// lock is taken over (as after a process stall past LockTTL), the handler's
// context must be cancelled so two handlers never run concurrently.
func TestLockLossCancelsRun(t *testing.T) {
	c := newTestClient(t)
	cfg := fastCfg(t, c)

	started := make(chan string, 1) // carries the session ID once running
	var cancelled atomic.Bool
	cfg.Handler = func(ctx context.Context, e *Event) error {
		select {
		case started <- e.SessionID:
		default:
		}
		select {
		case <-ctx.Done():
			cancelled.Store(true)
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return errors.New("handler was never cancelled")
		}
	}

	r, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Enqueue(t.Context(), "hijacked"); err != nil {
		t.Fatal(err)
	}

	go r.Run(t.Context())

	var sid string
	select {
	case sid = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	// Simulate lock expiry + takeover by another run: overwrite the lock
	// with a foreign token. The next renewal must detect the loss and
	// cancel the handler.
	err = c.Do(t.Context(), c.B().Set().Key(r.lockKey(sid)).Value("intruder").Build()).Error()
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, 5*time.Second, "handler cancellation after lock loss", func() bool {
		return cancelled.Load()
	})
}

func TestDifferentSessionsRunConcurrently(t *testing.T) {
	c := newTestClient(t)
	cfg := fastCfg(t, c)

	var mu sync.Mutex
	var running, maxRunning int
	var done atomic.Int32
	cfg.Handler = func(ctx context.Context, e *Event) error {
		mu.Lock()
		running++
		if running > maxRunning {
			maxRunning = running
		}
		mu.Unlock()
		time.Sleep(200 * time.Millisecond)
		mu.Lock()
		running--
		mu.Unlock()
		done.Add(1)
		return nil
	}

	r, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	r.Enqueue(t.Context(), "a")
	r.Enqueue(t.Context(), "b")
	r.Enqueue(t.Context(), "c")

	go r.Run(t.Context())

	waitFor(t, 10*time.Second, "all runs to finish", func() bool { return done.Load() == 3 })
	mu.Lock()
	defer mu.Unlock()
	if maxRunning < 2 {
		t.Errorf("expected different sessions to run concurrently, max concurrent = %d", maxRunning)
	}
}

// TestCrashedRunnerRecovered simulates a session reader/runner that dies
// mid-flight: a foreign consumer XREADGROUPs the entry and never processes
// it (no heartbeat, no ack). A live runner must autoclaim and finish it.
func TestCrashedRunnerRecovered(t *testing.T) {
	c := newTestClient(t)
	cfg := fastCfg(t, c)

	var ran atomic.Int32
	cfg.Handler = func(ctx context.Context, e *Event) error {
		ran.Add(1)
		return nil
	}

	r, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Enqueue(t.Context(), "victim"); err != nil {
		t.Fatal(err)
	}

	// "Crashing" consumer grabs the entry and disappears.
	res := c.Do(t.Context(), c.B().Xreadgroup().Group(cfg.Group, "dead-consumer").
		Count(1).Streams().Key(r.Stream()).Id(">").Build())
	if err := res.Error(); err != nil {
		t.Fatalf("simulating dead consumer: %v", err)
	}

	go r.Run(t.Context())

	waitFor(t, 10*time.Second, "recovery of the crashed session", func() bool {
		return ran.Load() == 1 && pendingCount(t, c, r) == 0
	})
}

// TestLongRunNotStolen verifies the heartbeat: a run much longer than
// ClaimMinIdle must not be redelivered to a second runner.
func TestLongRunNotStolen(t *testing.T) {
	c := newTestClient(t)
	cfg := fastCfg(t, c)

	var ran atomic.Int32
	cfg.Handler = func(ctx context.Context, e *Event) error {
		ran.Add(1)
		time.Sleep(4 * cfg.ClaimMinIdle) // far longer than the steal threshold
		return nil
	}

	r, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	cfg2 := cfg
	cfg2.Consumer = "second-runner"
	r2, err := New(t.Context(), cfg2)
	if err != nil {
		t.Fatal(err)
	}

	r.Enqueue(t.Context(), "long")

	go r.Run(t.Context())
	go r2.Run(t.Context())

	waitFor(t, 10*time.Second, "long run to finish and ack", func() bool {
		return pendingCount(t, c, r) == 0 && ran.Load() > 0
	})
	if n := ran.Load(); n != 1 {
		t.Errorf("handler ran %d times, want exactly 1 (entry was stolen mid-run)", n)
	}
}

func TestFailingSessionDeadLettered(t *testing.T) {
	c := newTestClient(t)
	cfg := fastCfg(t, c)

	var attempts, cleanups atomic.Int32
	var deadErr atomic.Value
	cfg.Handler = func(ctx context.Context, e *Event) error {
		attempts.Add(1)
		return errors.New("boom")
	}
	cfg.Cleanup = func(ctx context.Context, e *Event, runErr error) {
		cleanups.Add(1)
		if errors.Is(runErr, ErrDeadLettered) {
			deadErr.Store(runErr)
		}
	}

	r, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	r.Enqueue(t.Context(), "doomed")

	go r.Run(t.Context())

	waitFor(t, 15*time.Second, "dead-lettering", func() bool {
		return streamLen(t, c, r.DeadLetterStream()) == 1 && pendingCount(t, c, r) == 0
	})
	if got := attempts.Load(); int64(got) != cfg.MaxDeliveries {
		t.Errorf("handler attempts = %d, want %d", got, cfg.MaxDeliveries)
	}
	if deadErr.Load() == nil {
		t.Error("cleanup never received ErrDeadLettered")
	}
	// Cleanup must fire only when the run concludes — once, for the
	// dead-lettering — not after each retried failure.
	if n := cleanups.Load(); n != 1 {
		t.Errorf("cleanup ran %d times, want 1 (dead-letter only)", n)
	}
}

// TestDoneMarkerSkipsHandler simulates a crash after the handler succeeded
// but before the ack: the done marker exists, so the redelivery must skip
// the handler, still run cleanup, and ack.
func TestDoneMarkerSkipsHandler(t *testing.T) {
	c := newTestClient(t)
	cfg := fastCfg(t, c)

	var ran, cleaned atomic.Int32
	var skipped atomic.Bool
	cfg.Handler = func(ctx context.Context, e *Event) error {
		ran.Add(1)
		return nil
	}
	cfg.Cleanup = func(ctx context.Context, e *Event, runErr error) {
		skipped.Store(e.HandlerSkipped)
		cleaned.Add(1)
	}

	r, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	entryID, err := r.Enqueue(t.Context(), "resumed")
	if err != nil {
		t.Fatal(err)
	}
	// Simulate the prior attempt's success record.
	if err := r.setDoneMarker(t.Context(), entryID); err != nil {
		t.Fatal(err)
	}

	go r.Run(t.Context())

	waitFor(t, 5*time.Second, "cleanup and ack of the resumed session", func() bool {
		return cleaned.Load() == 1 && pendingCount(t, c, r) == 0
	})
	if ran.Load() != 0 {
		t.Errorf("handler ran %d times, want 0 (done marker should skip it)", ran.Load())
	}
	if !skipped.Load() {
		t.Error("cleanup did not see HandlerSkipped=true")
	}
	// Marker must be cleared after the ack.
	n, err := c.Do(t.Context(), c.B().Exists().Key(r.doneKey(entryID)).Build()).AsInt64()
	if err != nil || n != 0 {
		t.Errorf("done marker still present after ack (exists=%d, err=%v)", n, err)
	}
}

func TestHandlerPanicIsRetriedNotFatal(t *testing.T) {
	c := newTestClient(t)
	cfg := fastCfg(t, c)

	var attempts atomic.Int32
	cfg.Handler = func(ctx context.Context, e *Event) error {
		if attempts.Add(1) == 1 {
			panic("kaboom")
		}
		return nil
	}

	r, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	r.Enqueue(t.Context(), "panicky")

	go r.Run(t.Context())

	waitFor(t, 10*time.Second, "retry after panic", func() bool {
		return attempts.Load() >= 2 && pendingCount(t, c, r) == 0
	})
}

func pendingCount(t *testing.T, c valkey.Client, r *Runner) int64 {
	t.Helper()
	res := c.Do(context.Background(), c.B().Xpending().Key(r.Stream()).Group(r.cfg.Group).Build())
	arr, err := res.ToArray()
	if err != nil || len(arr) == 0 {
		return -1
	}
	n, err := arr[0].AsInt64()
	if err != nil {
		return -1
	}
	return n
}

func streamLen(t *testing.T, c valkey.Client, key string) int64 {
	t.Helper()
	n, err := c.Do(context.Background(), c.B().Xlen().Key(key).Build()).AsInt64()
	if err != nil {
		return -1
	}
	return n
}
