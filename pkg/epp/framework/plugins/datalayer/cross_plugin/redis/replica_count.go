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
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// replicaObservation is the live replica count one endpoint's hash held when it
// was last read, and when that read happened.
type replicaObservation struct {
	count int64
	at    time.Time
}

// replicaCounts tracks the live replica count across the endpoint hashes this
// replica reads, and when a read last merged peer values. See observeReplicas
// and PeerLoadFresh.
type replicaCounts struct {
	// active is the count ActiveReplicas reports.
	active atomic.Int64
	// lastPeerRead is when this replica last read or merged peer values, in
	// unix nanoseconds. It is what PeerLoadFresh compares against.
	lastPeerRead atomic.Int64

	mu sync.Mutex
	// seen is the last observation of each endpoint's hash, keyed by hash key.
	seen map[string]replicaObservation
	// computed is when active was last derived, which bounds how long a stale
	// observation can keep counting.
	computed time.Time
}

// PeerLoadFresh implements fwkdl.PeerLoadReporter. Peer values count as fresh
// while a hash read succeeded within the in-flight TTL, the TTL after which a
// peer's own in-flight value expires: a publication cycle that fails for longer
// than that leaves the aggregate with neither live peers nor a recent read.
//
// The in-flight TTL is the right window rather than the prefix TTL. What this
// reports is whether the peer load the detector compares against is complete,
// and only in-flight accounting expires on that short TTL. A retained prefix
// window outliving its replica on the prefix TTL says nothing about whether
// that replica's load still counts.
func (s *RedisSyncer) PeerLoadFresh() bool {
	last := s.replicas.lastPeerRead.Load()
	return last != 0 && time.Since(time.Unix(0, last)) <= s.inflightTTL()
}

// observeReplicas records how many replicas the read of one endpoint's hash
// found live, this replica included, and marks the read as the latest refresh of
// peer state.
//
// Replicas are counted from the per-replica liveness fields, by the same
// liveSet readers use, rather than from the value fields alone. Prefix windows
// are retained on the prefix TTL so a restarted replica can restore them, which
// means a hash can name a departed replica for many minutes after it stopped
// serving. Counting that window would inflate the divisor a flow-control budget
// divides by, and the budget would shrink for a replica that no longer takes
// traffic. A hash with no liveness field at all predates the field, and every
// replica it names still counts, which is how such a hash is read everywhere
// else.
//
// Every replica publishes the same endpoints, so any one endpoint's hash holds
// the whole live replica set. The count reported is still the largest across the
// hashes read within the in-flight TTL, because a read can catch a peer's expiry
// in one hash before another, and the largest count keeps that from shrinking
// the budget before the rest agree. An observation older than that window stops
// counting, so an endpoint this replica no longer publishes does not pin its
// peers.
func (s *RedisSyncer) observeReplicas(hashKey string, raw map[string]string) {
	now := time.Now()
	count := int64(s.liveReplicas(raw))
	s.replicas.lastPeerRead.Store(now.UnixNano())

	window := s.inflightTTL()
	s.replicas.mu.Lock()
	defer s.replicas.mu.Unlock()
	if s.replicas.seen == nil {
		s.replicas.seen = map[string]replicaObservation{}
	}
	previous, seen := s.replicas.seen[hashKey]
	s.replicas.seen[hashKey] = replicaObservation{count: count, at: now}
	if seen && previous.count == count && now.Sub(s.replicas.computed) <= window {
		// Steady state: this endpoint still sees the same replica set and the
		// reported count is recent, so there is nothing to re-derive.
		return
	}

	live := int64(0)
	for key, observation := range s.replicas.seen {
		if now.Sub(observation.at) > window {
			delete(s.replicas.seen, key)
			continue
		}
		live = max(live, observation.count)
	}
	s.replicas.computed = now
	s.replicas.active.Store(live)
}

// liveReplicas counts the replicas a hash reports as live, this replica
// included. A replica names itself through a whole-value field, a stream marker
// field, or its liveness field; the union is taken so a replica that publishes
// only one kind still counts, and the liveSet decides which of them are live.
//
// This replica counts itself unconditionally. It is live by definition, and if
// its own liveness field were missed the count would come up one short and the
// budget would divide by too few.
func (s *RedisSyncer) liveReplicas(raw map[string]string) int {
	live := newLiveSet(raw)
	ids := make(map[string]struct{}, len(raw)+1)
	ids[s.replicaID] = struct{}{}
	for field := range raw {
		if id, ok := strings.CutPrefix(field, markerPrefix); ok {
			field = id
		}
		if id, ok := strings.CutPrefix(field, refreshPrefix); ok {
			field = id
		}
		if live.isLive(field) {
			ids[field] = struct{}{}
		}
	}
	return len(ids)
}

// ActiveReplicas implements fwkdl.ReplicaCounter. It reports the last observed
// count without accessing Redis, so while Redis is unreachable the count stays
// where it was instead of collapsing to this replica alone.
func (s *RedisSyncer) ActiveReplicas() (int, bool) {
	live := s.replicas.active.Load()
	if live <= 0 {
		return 0, false
	}
	return int(live), true
}
