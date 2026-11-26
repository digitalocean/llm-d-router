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
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
)

// newWarmStreamReplica builds an incremental replica with the split TTLs a
// deployment gets: a short in-flight TTL and the long default prefix TTL.
func newWarmStreamReplica(t *testing.T, server *miniredis.Miniredis, replicaID string) *streamReplica {
	t.Helper()
	r := newStreamReplica(t, server, replicaID, true)
	r.syncer.inflightStateTTL = 2 * time.Second
	r.syncer.prefixStateTTL = 10 * time.Minute
	r.syncer.warmStartTimeout = 5 * time.Second
	r.syncer.peerGauges = newPeerGauges()
	return r
}

// warmStart runs a warm start for the replica's own spec and endpoint.
func (r *streamReplica) warmStart(t *testing.T) fwkdl.WarmStartResult {
	t.Helper()
	return r.syncer.WarmStart(context.Background(), []fwkdl.CrossReplicaSpec{r.spec()}, []string{testEndpointID})
}

func TestWarmStartMergesPeerWindowsBeforeFirstPublish(t *testing.T) {
	server := miniredis.RunT(t)
	b := newWarmStreamReplica(t, server, "epp-b")
	b.append([]uint64{1, 2}, []uint64{3})
	b.publish(t)

	// The starting replica has an empty local window and reads peer state
	// instead of waiting for traffic to arrive.
	a := newWarmStreamReplica(t, server, "epp-a")
	result := a.warmStart(t)

	require.NoError(t, result.Err)
	require.Equal(t, 1, result.Endpoints)
	require.Equal(t, 1, result.Peers)
	require.Equal(t, [][]uint64{{1, 2}, {3}}, a.peers(t),
		"the peer window is merged before this replica publishes")
}

func TestWarmStartIsBoundedByItsTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())

	plugin, err := RedisSyncerFactory("redis", json.NewDecoder(strings.NewReader(
		`{"address": "`+address+`", "warmStartTimeout": "200ms"}`,
	)), nil)
	require.NoError(t, err)
	syncer := plugin.(*RedisSyncer)
	t.Cleanup(func() { require.NoError(t, syncer.client.Close()) })

	window := &attrprefix.PrefixHashSet{}
	spec := fwkdl.CrossReplicaSpec{
		StateKey:  streamStateKey,
		Read:      func(string) fwkdl.Cloneable { return window },
		Aggregate: func(values []any) any { return values[0] },
	}
	started := time.Now()
	result := syncer.WarmStart(context.Background(), []fwkdl.CrossReplicaSpec{spec}, []string{testEndpointID})

	require.Error(t, result.Err, "an unreachable Valkey is reported")
	require.Less(t, time.Since(started), 5*time.Second, "the warm start is bounded by warmStartTimeout")
}

func TestWarmStartWithoutEndpointsIsANoop(t *testing.T) {
	server := miniredis.RunT(t)
	a := newWarmStreamReplica(t, server, "epp-a")

	result := a.syncer.WarmStart(context.Background(), []fwkdl.CrossReplicaSpec{a.spec()}, nil)

	require.NoError(t, result.Err)
	require.Zero(t, result.Endpoints)
	require.Zero(t, result.Peers)
}

func TestWarmStartRecordsMetrics(t *testing.T) {
	server := miniredis.RunT(t)
	b := newWarmStreamReplica(t, server, "epp-b")
	b.append([]uint64{1})
	b.publish(t)

	warmStartTotal.Reset()
	warmStartDuration.Reset()
	warmStartPeers.Set(0)
	a := newWarmStreamReplica(t, server, "epp-a")
	require.NoError(t, a.warmStart(t).Err)

	require.Equal(t, 1., testutil.ToFloat64(warmStartTotal.WithLabelValues(warmStartCompleted)))
	require.Equal(t, 1, testutil.CollectAndCount(warmStartDuration))
	require.Equal(t, 1., testutil.ToFloat64(warmStartPeers))
}

// TestBothReplicasRestartingRestoresTheIndexFromValkey is the correlated-crash
// case: both EPP processes are gone at once while Valkey stays up. Each replica's
// departure must leave its window in Valkey, and each replacement must have peer
// state merged before it reports ready.
func TestBothReplicasRestartingRestoresTheIndexFromValkey(t *testing.T) {
	server := miniredis.RunT(t)
	a := newWarmStreamReplica(t, server, "epp-a")
	b := newWarmStreamReplica(t, server, "epp-b")
	a.append([]uint64{1, 2})
	b.append([]uint64{3, 4})
	a.publish(t)
	b.publish(t)
	ctx := context.Background()

	// Both replicas depart. Their windows stay in Valkey.
	require.NoError(t, a.syncer.Delete(ctx, streamStateKey, testEndpointID))
	require.NoError(t, b.syncer.Delete(ctx, streamStateKey, testEndpointID))
	require.True(t, server.Exists(a.streamKey()))
	require.True(t, server.Exists(b.streamKey()))

	// Each process starts again as a new replica with an empty local window.
	freshA := newWarmStreamReplica(t, server, "epp-a-2")
	freshB := newWarmStreamReplica(t, server, "epp-b-2")
	for _, fresh := range []*streamReplica{freshA, freshB} {
		result := fresh.warmStart(t)
		require.NoError(t, result.Err)
		require.Equal(t, 2, result.Peers, "both retained windows are restored")
	}

	// A replica's own past window comes back as peer state, which is what the
	// aggregate means: the union of recent prompts every replica recorded.
	for _, fresh := range []*streamReplica{freshA, freshB} {
		require.ElementsMatch(t, [][]uint64{{1, 2}, {3, 4}}, fresh.peers(t),
			"the index is restored from Valkey before the replica reports ready")
	}
}

func TestDepartedReplicaStopsCountingAsAnActivePeer(t *testing.T) {
	server := miniredis.RunT(t)
	a := newWarmStreamReplica(t, server, "epp-a")
	b := newWarmStreamReplica(t, server, "epp-b")
	a.append([]uint64{1})
	b.append([]uint64{2})
	activePeers.Reset()
	a.publish(t)
	b.publish(t)

	require.Equal(t, 1., testutil.ToFloat64(activePeers.WithLabelValues(string(streamStateKey))))

	// b departs: its window is retained, this replica stops counting it.
	require.NoError(t, b.syncer.Delete(context.Background(), streamStateKey, testEndpointID))
	a.publish(t)

	require.Zero(t, testutil.ToFloat64(activePeers.WithLabelValues(string(streamStateKey))),
		"a retained window from a departed replica is not an active peer")
	require.Equal(t, [][]uint64{{2}}, a.peers(t),
		"its prompts stay merged while the window is retained")
}

func TestRetainedWindowExpiresOnThePrefixTTL(t *testing.T) {
	server := miniredis.RunT(t)
	a := newWarmStreamReplica(t, server, "epp-a")
	a.append([]uint64{1})
	a.publish(t)
	streamKey := a.streamKey()

	require.NoError(t, a.syncer.Delete(context.Background(), streamStateKey, testEndpointID))
	require.True(t, server.Exists(streamKey))
	require.Equal(t, a.syncer.prefixStateTTL, server.TTL(streamKey))

	server.FastForward(a.syncer.prefixStateTTL + time.Second)
	require.False(t, server.Exists(streamKey), "the retained window is garbage-collected on its TTL")
}

func TestDeleteRemovesAccountingValues(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newWarmStreamReplica(t, server, "epp-a").syncer
	ctx := context.Background()

	require.NoError(t, syncer.set(ctx, testStateKey, testEndpointID, 4, sumInts))
	require.NoError(t, syncer.Delete(ctx, testStateKey, testEndpointID))

	exists, err := syncer.client.HExists(ctx, syncer.hashKey(testStateKey, testEndpointID), "epp-a").Result()
	require.NoError(t, err)
	require.False(t, exists, "in-flight accounting is not retained")
}

func TestLivenessFieldExpiresWhileTheWindowIsRetained(t *testing.T) {
	server := miniredis.RunT(t)
	a := newWarmStreamReplica(t, server, "epp-a")
	b := newWarmStreamReplica(t, server, "epp-b")
	a.append([]uint64{1})
	b.append([]uint64{2})
	a.publish(t)
	b.publish(t)
	ctx := context.Background()

	// b stops publishing. Past its in-flight TTL it no longer looks live, while
	// its stream stays for the prefix TTL.
	server.FastForward(b.syncer.inflightStateTTL + time.Second)
	exists, err := a.syncer.client.HExists(ctx, b.hashKey(), refreshField("epp-b")).Result()
	require.NoError(t, err)
	require.False(t, exists, "a replica that stopped publishing stops being live")
	require.True(t, server.Exists(b.streamKey()), "its window is still retained")

	a.publish(t)
	require.Zero(t, testutil.ToFloat64(activePeers.WithLabelValues(string(streamStateKey))))
	require.Equal(t, [][]uint64{{2}}, a.peers(t))
}

func TestHashesFromBeforeLivenessCountAsLive(t *testing.T) {
	server := miniredis.RunT(t)
	peer := newWarmStreamReplica(t, server, "epp-b")
	peer.append([]uint64{1})
	peer.publish(t)
	// Simulate a peer from a version without the liveness field.
	require.NoError(t, peer.syncer.client.HDel(context.Background(), peer.hashKey(), refreshField("epp-b")).Err())

	local := newWarmStreamReplica(t, server, "epp-a")
	local.publish(t)

	require.Equal(t, [][]uint64{{1}}, local.peers(t),
		"a hash without a liveness field treats every replica it names as live")
}
