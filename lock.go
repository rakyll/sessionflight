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
	"time"

	"github.com/valkey-io/valkey-go"
)

// Per-session-ID mutual exclusion. The lock value is a token unique to one
// run attempt so a worker can only renew or release a lock it still owns —
// a lock that expired and was re-acquired by someone else is never touched.

var renewScript = valkey.NewLuaScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0`)

var releaseScript = valkey.NewLuaScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0`)

// acquireLock polls until the session lock is held, ctx is cancelled, or an
// unexpected error occurs. Contention here is exactly the "same session_id
// back to back" case: an earlier run of this session is still in flight.
func (r *Runner) acquireLock(ctx context.Context, sessionID, token string) error {
	c := r.cfg.Client
	key := r.lockKey(sessionID)
	for {
		res := c.Do(ctx, c.B().Set().Key(key).Value(token).Nx().Px(r.cfg.LockTTL).Build())
		err := res.Error()
		if err == nil {
			return nil // "OK" — lock acquired
		}
		if !valkey.IsValkeyNil(err) {
			return fmt.Errorf("acquiring session lock %s: %w", key, err)
		}
		// Held by another run; wait and retry.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.cfg.LockRetryInterval):
		}
	}
}

// errLockLost means the lock definitively no longer belongs to this run:
// it expired or was taken over by another run.
var errLockLost = errors.New("sessionflight: session lock lost")

func (r *Runner) renewLock(ctx context.Context, sessionID, token string) error {
	key := r.lockKey(sessionID)
	res := renewScript.Exec(ctx, r.cfg.Client,
		[]string{key},
		[]string{token, fmt.Sprintf("%d", r.cfg.LockTTL.Milliseconds())})
	n, err := res.AsInt64()
	if err != nil {
		return fmt.Errorf("renewing session lock %s: %w", key, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: %s", errLockLost, key)
	}
	return nil
}

func (r *Runner) releaseLock(ctx context.Context, sessionID, token string) {
	key := r.lockKey(sessionID)
	res := releaseScript.Exec(ctx, r.cfg.Client, []string{key}, []string{token})
	if err := res.Error(); err != nil {
		// Non-fatal: the lock has a TTL and will expire on its own.
		r.cfg.Logger.Warn("sessionflight: releasing session lock failed; it will expire via TTL",
			"key", key, "err", err)
	}
}
