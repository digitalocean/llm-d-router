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
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

// newReplicaCountSyncer returns a syncer whose replica observations can be aged
// by the tests.
func newReplicaCountSyncer(t *testing.T, server *miniredis.Miniredis, replicaID string) *RedisSyncer {
	t.Helper()
	syncer, _ := newTestSyncer(t, server, replicaID)
	return syncer
}

// ageReplicaObservations moves this syncer's replica observations d into the
// past, as if that much time had passed since they were recorded.
func ageReplicaObservations(syncer *RedisSyncer, d time.Duration) {
	syncer.replicas.mu.Lock()
	defer syncer.replicas.mu.Unlock()
	for key, observation := range syncer.replicas.seen {
		observation.at = observation.at.Add(-d)
		syncer.replicas.seen[key] = observation
	}
	syncer.replicas.computed = syncer.replicas.computed.Add(-d)
}

// TestActiveReplicasUnknownBeforeFirstSet verifies that no count is reported
// before this replica publishes.
func TestActiveReplicasUnknownBeforeFirstSet(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	syncer := newReplicaCountSyncer(t, server, testReplicaID)

	count, ok := syncer.ActiveReplicas()
	require.False(t, ok, "no publication has been seen yet")
	require.Zero(t, count)
}

// TestActiveReplicasCountsSelfAndPeers verifies the count is this replica plus
// every peer whose field is still live.
func TestActiveReplicasCountsSelfAndPeers(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	syncer := newReplicaCountSyncer(t, server, testReplicaID)
	ctx := context.Background()

	require.NoError(t, syncer.set(ctx, testStateKey, testEndpointID, 1, sumInts))
	count, ok := syncer.ActiveReplicas()
	require.True(t, ok)
	require.Equal(t, 1, count, "a lone replica counts once")

	seedReplica(t, server, testStateKey, "epp-b", 1)
	seedReplica(t, server, testStateKey, "epp-c", 1)
	require.NoError(t, syncer.set(ctx, testStateKey, testEndpointID, 1, sumInts))
	count, _ = syncer.ActiveReplicas()
	require.Equal(t, 3, count)
}

// TestActiveReplicasDropsExpiredPeer verifies a peer leaves the count only after
// its heartbeat expires.
func TestActiveReplicasDropsExpiredPeer(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	syncer := newReplicaCountSyncer(t, server, testReplicaID)
	ctx := context.Background()

	seedReplica(t, server, testStateKey, "epp-b", 1)
	require.NoError(t, syncer.set(ctx, testStateKey, testEndpointID, 1, sumInts))
	count, _ := syncer.ActiveReplicas()
	require.Equal(t, 2, count)

	// The peer stops publishing. Its field lives until its TTL, so it keeps
	// counting until then.
	require.NoError(t, syncer.set(ctx, testStateKey, testEndpointID, 1, sumInts))
	count, _ = syncer.ActiveReplicas()
	require.Equal(t, 2, count, "a silent peer keeps counting until its heartbeat expires")

	// Its heartbeat expires and the next read finds only this replica.
	server.FastForward(2 * testStateTTL)
	require.NoError(t, syncer.set(ctx, testStateKey, testEndpointID, 1, sumInts))
	count, _ = syncer.ActiveReplicas()
	require.Equal(t, 1, count)
}

// TestActiveReplicasPeerJoins verifies a new peer joins the count on its first
// publication.
func TestActiveReplicasPeerJoins(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	syncer := newReplicaCountSyncer(t, server, testReplicaID)
	peer := newReplicaCountSyncer(t, server, "epp-b")
	ctx := context.Background()

	require.NoError(t, syncer.set(ctx, testStateKey, testEndpointID, 1, sumInts))
	count, _ := syncer.ActiveReplicas()
	require.Equal(t, 1, count)

	require.NoError(t, peer.set(ctx, testStateKey, testEndpointID, 1, sumInts))
	require.NoError(t, syncer.set(ctx, testStateKey, testEndpointID, 1, sumInts))
	count, _ = syncer.ActiveReplicas()
	require.Equal(t, 2, count, "the joining replica is counted once it publishes")
}

// TestActiveReplicasUnreachableStoreKeepsCount verifies the count survives a
// failed publication: a Set that cannot reach Redis leaves it unchanged.
func TestActiveReplicasUnreachableStoreKeepsCount(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	syncer := newReplicaCountSyncer(t, server, testReplicaID)
	ctx := context.Background()

	seedReplica(t, server, testStateKey, "epp-b", 1)
	require.NoError(t, syncer.set(ctx, testStateKey, testEndpointID, 1, sumInts))
	count, _ := syncer.ActiveReplicas()
	require.Equal(t, 2, count)

	// Redis becomes unreachable. The publication fails and the count stays.
	server.Close()
	require.Error(t, syncer.set(ctx, testStateKey, testEndpointID, 1, sumInts))
	count, ok := syncer.ActiveReplicas()
	require.True(t, ok, "the last observed count is still reported")
	require.Equal(t, 2, count, "an unreachable store must not shrink the count to this replica")
}

// TestObserveReplicasCountsReplicasNotFields verifies a replica that publishes
// both a whole value and a stream marker counts once.
func TestObserveReplicasCountsReplicasNotFields(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	syncer := newReplicaCountSyncer(t, server, testReplicaID)

	// epp-b is mid-migration from full to incremental replication, so it holds
	// both a value field and a stream marker field in the same hash.
	syncer.observeReplicas(syncer.hashKey(testStateKey, testEndpointID), map[string]string{
		testReplicaID:        "",
		"epp-b":              "",
		markerField("epp-b"): "",
	})

	count, ok := syncer.ActiveReplicas()
	require.True(t, ok)
	require.Equal(t, 2, count)
}

// TestObserveReplicasUsesLargestCount verifies the count is the largest across
// the endpoints read within the window, so a peer's expiry seen on one endpoint
// does not shrink the budget before the others agree.
func TestObserveReplicasUsesLargestCount(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	syncer := newReplicaCountSyncer(t, server, testReplicaID)

	syncer.observeReplicas(syncer.hashKey(testStateKey, testEndpointID), map[string]string{testReplicaID: "", "epp-b": ""})
	syncer.observeReplicas(syncer.hashKey(testStateKey, "backend-rank-1"), map[string]string{testReplicaID: ""})

	count, ok := syncer.ActiveReplicas()
	require.True(t, ok)
	require.Equal(t, 2, count)
}

// TestObserveReplicasDropsStaleObservation verifies that an endpoint this
// replica stopped publishing does not pin its peers forever.
func TestObserveReplicasDropsStaleObservation(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	syncer := newReplicaCountSyncer(t, server, testReplicaID)

	syncer.observeReplicas(syncer.hashKey(testStateKey, testEndpointID), map[string]string{testReplicaID: "", "epp-b": ""})
	syncer.observeReplicas(syncer.hashKey(testStateKey, "backend-rank-1"), map[string]string{testReplicaID: ""})
	count, _ := syncer.ActiveReplicas()
	require.Equal(t, 2, count)

	// Only the endpoint without peers is read from now on.
	ageReplicaObservations(syncer, 2*testStateTTL)
	syncer.observeReplicas(syncer.hashKey(testStateKey, "backend-rank-1"), map[string]string{testReplicaID: ""})
	count, _ = syncer.ActiveReplicas()
	require.Equal(t, 1, count)
}

// TestActiveReplicasIgnoresRetainedWindow verifies a departed replica whose
// prefix window is retained on the long prefix TTL stops counting as active once
// its liveness field expires. Counting the window would shrink every surviving
// replica's flow-control budget for a replica that no longer takes traffic.
func TestActiveReplicasIgnoresRetainedWindow(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	syncer := newReplicaCountSyncer(t, server, testReplicaID)
	// The split-TTL defaults, which is the case the retention exists for.
	syncer.inflightStateTTL = 2 * time.Second
	syncer.prefixStateTTL = 10 * time.Minute
	ctx := context.Background()
	hashKey := syncer.hashKey(testStateKey, testEndpointID)

	// epp-b published a prefix window and its liveness field, then departed.
	// The window and marker stay on the prefix TTL while the liveness field
	// expires on the in-flight TTL.
	seedLivePeer(t, syncer, hashKey, "epp-b", syncer.prefixTTL())
	syncer.observeReplicas(hashKey, rawHash(t, syncer, hashKey))
	count, _ := syncer.ActiveReplicas()
	require.Equal(t, 2, count, "a replica that refreshed within the in-flight TTL counts")

	// The liveness field expires; the retained marker and window remain.
	require.NoError(t, syncer.client.HDel(ctx, hashKey, refreshField("epp-b")).Err())
	syncer.observeReplicas(hashKey, rawHash(t, syncer, hashKey))
	count, _ = syncer.ActiveReplicas()
	require.Equal(t, 1, count, "a retained window is not an active replica")

	// The retained window is still readable, so a restarted replica could
	// restore it; it is only the divisor that dropped it.
	require.Contains(t, hashFields(t, server, hashKey), markerField("epp-b"))
}

// rawHash reads a hash the way a publication's HGETALL does.
func rawHash(t *testing.T, syncer *RedisSyncer, hashKey string) map[string]string {
	t.Helper()
	raw, err := syncer.client.HGetAll(context.Background(), hashKey).Result()
	require.NoError(t, err)
	return raw
}

// seedLivePeer writes a peer's prefix marker and liveness field into hashKey,
// with the window on prefixTTL and the liveness field on the in-flight TTL, the
// way a prefix publication leaves them. This replica's own liveness field is
// written too, so the hash carries the liveness form at all: without any such
// field a hash reads as one written before the field existed, where every
// replica it names is live.
func seedLivePeer(t *testing.T, syncer *RedisSyncer, hashKey, replicaID string, prefixTTL time.Duration) {
	t.Helper()
	ctx := context.Background()
	pipe := syncer.client.TxPipeline()
	pipe.HSet(ctx, hashKey, markerField(replicaID), formatMarker(streamCursor{epoch: 1}))
	pipe.HPExpire(ctx, hashKey, prefixTTL, markerField(replicaID))
	pipe.HSet(ctx, hashKey, refreshField(replicaID), refreshValue)
	pipe.HPExpire(ctx, hashKey, syncer.inflightTTL(), refreshField(replicaID))
	pipe.HSet(ctx, hashKey, refreshField(syncer.replicaID), refreshValue)
	pipe.HPExpire(ctx, hashKey, syncer.inflightTTL(), refreshField(syncer.replicaID))
	pipe.PExpire(ctx, hashKey, prefixTTL)
	_, err := pipe.Exec(ctx)
	require.NoError(t, err)
}

// TestActiveReplicasCountsBeforeLivenessField verifies a hash with no liveness
// field, written by a version that predates it, is read as it was before: every
// replica it names counts.
func TestActiveReplicasCountsBeforeLivenessField(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	syncer := newReplicaCountSyncer(t, server, testReplicaID)
	ctx := context.Background()

	seedReplica(t, server, testStateKey, "epp-b", 1)
	require.NoError(t, syncer.set(ctx, testStateKey, testEndpointID, 1, sumInts))

	hashKey := syncer.hashKey(testStateKey, testEndpointID)
	require.Empty(t, refreshFields(t, server, hashKey), "the seeded hash has no liveness field")

	count, ok := syncer.ActiveReplicas()
	require.True(t, ok)
	require.Equal(t, 2, count, "without liveness fields every named replica counts, as before")
}

// TestActiveReplicasMixedLiveness verifies a hash holding both forms is read per
// replica, the same way readers treat it: the replica with a liveness field is
// live, one named only by its value field is not.
func TestActiveReplicasMixedLiveness(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	syncer := newReplicaCountSyncer(t, server, testReplicaID)

	syncer.observeReplicas(syncer.hashKey(testStateKey, testEndpointID), map[string]string{
		testReplicaID:         "",
		refreshField("epp-b"): refreshValue,
		"epp-c":               "",
	})

	count, _ := syncer.ActiveReplicas()
	require.Equal(t, 2, count, "this replica and the one with a liveness field")
}

// TestReplicaObservationWindowUsesInflightTTL verifies the replica count window
// follows the in-flight TTL, not the longer prefix TTL: a prefix window outliving
// its replica says nothing about whether that replica still serves.
func TestReplicaObservationWindowUsesInflightTTL(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	syncer := newReplicaCountSyncer(t, server, testReplicaID)
	// A live configuration that sets only stateTTL keeps one TTL for everything.
	syncer.stateTTL = testStateTTL
	ctx := context.Background()

	seedReplica(t, server, testStateKey, "epp-b", 1)
	require.NoError(t, syncer.set(ctx, testStateKey, testEndpointID, 1, sumInts))
	count, _ := syncer.ActiveReplicas()
	require.Equal(t, 2, count)

	// Past the in-flight TTL the observation stops counting, even though a
	// prefix window would still be readable on a longer prefix TTL.
	ageReplicaObservations(syncer, 2*syncer.inflightTTL())
	server.FastForward(2 * testStateTTL)
	require.NoError(t, syncer.set(ctx, testStateKey, testEndpointID, 1, sumInts))
	count, _ = syncer.ActiveReplicas()
	require.Equal(t, 1, count)
}

// TestReplicaObservationFollowsSplitTTLs verifies that with the split-TTL
// defaults, where inflightStateTTL is far shorter than prefixStateTTL, the
// observation window is the short one.
func TestReplicaObservationFollowsSplitTTLs(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	syncer := newReplicaCountSyncer(t, server, testReplicaID)
	syncer.inflightStateTTL = 2 * time.Second
	syncer.prefixStateTTL = 10 * time.Minute
	require.Equal(t, 2*time.Second, syncer.inflightTTL())
	require.Equal(t, 10*time.Minute, syncer.prefixTTL())

	hashKey := syncer.hashKey(testStateKey, testEndpointID)
	seedLivePeer(t, syncer, hashKey, "epp-b", syncer.prefixTTL())
	syncer.observeReplicas(hashKey, rawHash(t, syncer, hashKey))
	count, _ := syncer.ActiveReplicas()
	require.Equal(t, 2, count)

	// Older than the in-flight TTL but well inside the prefix TTL, the
	// observation stops counting.
	ageReplicaObservations(syncer, 3*time.Second)
	syncer.observeReplicas(hashKey, map[string]string{testReplicaID: "", refreshField(testReplicaID): refreshValue})
	count, _ = syncer.ActiveReplicas()
	require.Equal(t, 1, count, "the observation window follows the in-flight TTL")
}

// refreshFields returns the liveness fields present in a hash.
func refreshFields(t *testing.T, server *miniredis.Miniredis, hashKey string) []string {
	t.Helper()
	var fields []string
	for _, field := range hashFields(t, server, hashKey) {
		if strings.HasPrefix(field, refreshPrefix) {
			fields = append(fields, field)
		}
	}
	return fields
}

// hashFields returns the fields present in a hash.
func hashFields(t *testing.T, server *miniredis.Miniredis, hashKey string) []string {
	t.Helper()
	fields, err := server.HKeys(hashKey)
	require.NoError(t, err)
	return fields
}

// TestActiveReplicasStreamPath verifies the count also comes from the stream
// read path, which caches peer aggregates separately.
func TestActiveReplicasStreamPath(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	reader := newStreamReplica(t, server, testReplicaID, true)
	reader.window.AppendContent([]uint64{1})
	reader.publish(t)

	count, ok := reader.syncer.ActiveReplicas()
	require.True(t, ok)
	require.Equal(t, 1, count, "one replica publishing a stream counts once")

	peer := newStreamReplica(t, server, "epp-b", true)
	peer.window.AppendContent([]uint64{2})
	peer.publish(t)
	reader.publish(t)

	count, _ = reader.syncer.ActiveReplicas()
	require.Equal(t, 2, count)
}
