# sessionflight

A Go library that orchestrates long-running session jobs on top of
[Valkey](https://valkey.io) streams.

You enqueue events and run one or more *runners* that consume them, execute your handler,
run a cleanup hook once the run concludes (success or dead-letter), and
acknowledge the event only once the run has actually finished.
The same `session_id` may be enqueued again
while an earlier run is still in flight; the runs never overlap.

## Usage

```go
client, err := valkey.NewClient(valkey.ClientOption{
    InitAddress: []string{"127.0.0.1:6379"},
})
if err != nil { ... }

r, err := sessionflight.New(ctx, sessionflight.Config{
    Client: client,
    Handler: func(ctx context.Context, e *sessionflight.Event) error {
        // Run the session. May take a long time; the runner heartbeats
        // for you. Return nil to acknowledge, an error to retry later.
        return runSession(ctx, e.SessionID)
    },
    Cleanup: func(ctx context.Context, e *sessionflight.Event, err error) {
        // Called when the run concludes: success (err == nil) or
        // dead-lettering (errors.Is(err, sessionflight.ErrDeadLettered)).
        // Not called for failed attempts that will be retried.
        cleanupSession(ctx, e.SessionID)
    },
})
if err != nil { ... }

// Producer side (can be the same or a different process):
entryID, err := r.Enqueue(ctx, "session-42")
entryID, err := r.Enqueue(ctx, "session-42")
entryID, err := r.Enqueue(ctx, "session-23")

// Worker side — blocks until ctx is cancelled:
err = r.Run(ctx)
```

Run as many runner processes as you like against the same `Prefix`/`Group`;
they share the work through a Valkey consumer group.

## Failure handling

| Failure | Handling strategy |
|---|---|
| Runner process crashes mid-run | The entry stays in the consumer group's pending list. Once its idle time exceeds `ClaimMinIdle` (no more heartbeats), another runner reclaims it with `XAUTOCLAIM` and re-runs the handler. |
| Reader dies between delivery and processing | Same as above: no heartbeat, so the entry is reclaimed. |
| Handler returns an error or panics | The event is not acknowledged and is retried after `ClaimMinIdle`; cleanup does not run for attempts that will be retried. After `MaxDeliveries` attempts (default 3) the event is copied to the dead-letter stream (`<prefix>:dead`), cleanup runs with `ErrDeadLettered`, and the entry is acknowledged. |
| Crash after the handler succeeded but before the ack | A completion marker records the success. The redelivery skips the handler (`Event.HandlerSkipped == true`), runs cleanup, and acknowledges. The handler is not run twice. |
| Same `session_id` enqueued 2+ times back to back | A per-session-ID lock guarantees at most one handler runs per session ID at any time, even across runner processes. Events for the same session delivered to the same runner execute in enqueue order; ordering across runners and across retries is best-effort. |
| Long run (longer than `ClaimMinIdle`) | The runner heartbeats every `HeartbeatInterval` (`XCLAIM JUSTID` + lock renewal), so live runs are never stolen, no matter how long they take. |
| Process stall past `LockTTL` (e.g. network partition) | The session lock expires and another run of that session may start. The stalled run's next lock renewal detects the loss — or renewals keep failing for longer than `LockTTL` — and the handler's context is cancelled. Handlers must honor their context; that is what upholds the single-run-per-session guarantee. The stalled run's event is not acked and is retried. |

Delivery is at-least-once end to end: the handler is effectively run once
per event in the common case, but cleanup may run more than once for the
same event after a crash — make cleanup idempotent.

## Keys used

Everything lives under `Config.Prefix` (default `sessionflight`):

- `<prefix>:stream` — the event stream (entries are deleted after ack)
- `<prefix>:dead` — dead-letter stream (fields: `sid`, `entry_id`, `attempts`, `reason`)
- `<prefix>:lock:<session_id>` — per-session run lock
- `<prefix>:done:<entry_id>` — "handler already succeeded" markers

## Tests

Tests run against an in-process [miniredis](https://github.com/alicebob/miniredis)
by default, so they need nothing but:

```sh
go test -race ./...
```

To run them against a real Valkey server instead:

```sh
VALKEY_ADDR=127.0.0.1:6379 go test -race ./...
```
