/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package redis

import (
	"context"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// refreshPrefix names the hash field that reports a replica as live. Every
// publication writes the field with the in-flight TTL, so its presence means
// the replica refreshed within inflightStateTTL.
//
// Prefix state is retained on the much longer prefixStateTTL, so a hash can hold
// a departed replica's window long after that replica stopped publishing. This
// field separates the two: the window stays readable so a restarted replica can
// restore it, while the peer stops counting as active once this field expires
// or Delete removes it.
const refreshPrefix = "refresh:"

// refreshValue is the liveness field's value. Only its presence and TTL matter.
const refreshValue = "1"

// refreshField names the liveness field for one replica.
func refreshField(replicaID string) string { return refreshPrefix + replicaID }

// liveSet is the set of replicas a hash reports as live.
type liveSet struct {
	ids map[string]struct{}
	// known is false for a hash written before the liveness field existed. Such
	// a hash cannot distinguish a live replica from a retained window, so every
	// replica it names counts as live, which is how peers were read before.
	known bool
}

// newLiveSet reads the replicas a hash reports as live.
func newLiveSet(raw map[string]string) liveSet {
	live := liveSet{ids: make(map[string]struct{})}
	for field := range raw {
		id, ok := strings.CutPrefix(field, refreshPrefix)
		if !ok {
			continue
		}
		live.ids[id] = struct{}{}
		live.known = true
	}
	return live
}

func (l liveSet) isLive(replicaID string) bool {
	if !l.known {
		return true
	}
	_, ok := l.ids[replicaID]
	return ok
}

// drainingPrefix names the hash field a replica writes once it is draining. A
// draining replica still publishes its in-flight load so peers see it finish,
// but it takes no new requests, so peers must stop dividing a budget by it.
// The liveness field alone cannot say that: in-flight hashes carry none, and
// a replica's value field there is what makes it count.
const drainingPrefix = "draining:"

// livenessCommands queues this replica's liveness refresh, or the draining
// marker in its place once the replica is draining.
func (s *RedisSyncer) livenessCommands(ctx context.Context, pipe goredis.Pipeliner, hashKey string) []goredis.Cmder {
	if s.draining.Load() {
		return s.drainCommands(ctx, pipe, hashKey)
	}
	return refreshCommands(ctx, pipe, hashKey, s.replicaID, s.inflightTTL())
}

// drainCommands writes and expires the draining marker, or nothing while the
// replica is not draining.
func (s *RedisSyncer) drainCommands(ctx context.Context, pipe goredis.Pipeliner, hashKey string) []goredis.Cmder {
	if !s.draining.Load() {
		return nil
	}
	field := drainingPrefix + s.replicaID
	return []goredis.Cmder{
		pipe.HSet(ctx, hashKey, field, refreshValue),
		pipe.HPExpire(ctx, hashKey, s.inflightTTL(), field),
	}
}

// StartDrain implements fwkdl.Drainer.
func (s *RedisSyncer) StartDrain() { s.draining.Store(true) }

// refreshCommands writes and expires this replica's liveness field. Repeating
// them on every publication tick is what keeps an idle replica live.
func refreshCommands(ctx context.Context, pipe goredis.Pipeliner, hashKey, replicaID string, ttl time.Duration) []goredis.Cmder {
	field := refreshField(replicaID)
	return []goredis.Cmder{
		pipe.HSet(ctx, hashKey, field, refreshValue),
		pipe.HPExpire(ctx, hashKey, ttl, field),
	}
}
