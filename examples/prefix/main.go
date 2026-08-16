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

// Command prefix demonstrates using Config.Prefix to namespace streams and
// isolate runners to a specific session ID or tenant.
//
// Run it with:
//
//	go run ./examples/prefix
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/alicebob/miniredis/v2"
	"github.com/rakyll/sessionflight"
	"github.com/valkey-io/valkey-go"
)

func main() {
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

	sessionID := "session-42"

	// Setting Prefix namespaces the stream, consumer group, and session lock
	// (e.g. "sessionflight:session-42:stream"), dedicating this runner exclusively
	// to events for session-42.
	runner, err := sessionflight.New(context.Background(), sessionflight.Config{
		Client: client,
		Prefix: fmt.Sprintf("sessionflight:%s", sessionID),
		Handler: func(ctx context.Context, e *sessionflight.Event) error {
			fmt.Printf("run     session=%s entry=%s\n", e.SessionID, e.EntryID)
			return nil
		},
		Cleanup: func(ctx context.Context, e *sessionflight.Event, err error) {
			fmt.Printf("cleanup session=%s entry=%s\n", e.SessionID, e.EntryID)
		},
	})
	_ = runner // use runner
}
