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

// Command helloworld is a self-contained sessionflight demo: it starts an
// in-process miniredis as the backing store, enqueues a few session run
// requests — including two for the same session ID, which are guaranteed
// to run one after the other — processes them, and exits.
//
// Run it with:
//
//	go run ./examples/helloworld
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/rakyll/sessionflight"
	"github.com/valkey-io/valkey-go"
)

func main() {
	// An in-process Redis-compatible server so the demo needs nothing
	// installed. In production, point the client at your Valkey server.
	mini, err := miniredis.Run()
	if err != nil {
		log.Fatalf("starting miniredis: %v", err)
	}
	defer mini.Close()

	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:  []string{mini.Addr()},
		DisableCache: true,
	})
	if err != nil {
		log.Fatalf("connecting: %v", err)
	}
	defer client.Close()

	sessions := []string{"alice", "bob", "alice"} // alice twice: runs back to back, never overlapping
	var done sync.WaitGroup
	done.Add(len(sessions))

	runner, err := sessionflight.New(context.Background(), sessionflight.Config{
		Client: client,
		Handler: func(ctx context.Context, e *sessionflight.Event) error {
			fmt.Printf("run     session=%s entry=%s attempt=%d\n", e.SessionID, e.EntryID, e.Attempt)
			time.Sleep(100 * time.Millisecond) // pretend this is long-running work
			fmt.Printf("finish  session=%s entry=%s\n", e.SessionID, e.EntryID)
			return nil // nil acknowledges the event; an error would retry it
		},
		Cleanup: func(ctx context.Context, e *sessionflight.Event, err error) {
			fmt.Printf("cleanup session=%s entry=%s err=%v\n", e.SessionID, e.EntryID, err)
			done.Done()
		},
		// Snappy timings so the demo reacts fast; the defaults (seconds,
		// not milliseconds) are better suited to real deployments.
		BlockTimeout:      200 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		ClaimMinIdle:      400 * time.Millisecond,
		AutoclaimInterval: 100 * time.Millisecond,
		LockTTL:           400 * time.Millisecond,
		LockRetryInterval: 20 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("creating runner: %v", err)
	}

	for _, sid := range sessions {
		entryID, err := runner.Enqueue(context.Background(), sid)
		if err != nil {
			log.Fatalf("enqueue %s: %v", sid, err)
		}
		fmt.Printf("enqueue session=%s entry=%s\n", sid, entryID)
	}

	// Run until every enqueued event has been run and cleaned up, then
	// shut the runner down by cancelling its context.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		done.Wait()
		cancel()
	}()
	if err := runner.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("runner: %v", err)
	}
	fmt.Println("all sessions finished")
}
