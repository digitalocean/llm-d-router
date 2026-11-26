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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/cross_plugin/internal/crossreplica"
)

func TestRegisterMetricsRegistersEveryCollector(t *testing.T) {
	// Touch one label set per vector so Gather reports every family.
	const key = "registered"
	streamBytes.WithLabelValues(key, directionPublish)
	valkeyCommands.WithLabelValues(key, resultSuccess)
	valkeyCommandDuration.WithLabelValues(key)
	activePeers.WithLabelValues(key)
	peerIndexEntries.WithLabelValues(key)
	lastSuccessfulSync.WithLabelValues(key)
	streamBudgetBytesPerSecond.WithLabelValues(key)

	registry := prometheus.NewRegistry()
	require.NoError(t, registerMetrics(registry))
	require.NoError(t, registerMetrics(registry), "re-registering the same collectors is tolerated")

	got, err := registry.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(got))
	for _, family := range got {
		names = append(names, family.GetName())
	}
	require.Subset(t, names, []string{
		"llm_d_epp_redis_syncer_stream_bytes_total",
		"llm_d_epp_redis_syncer_valkey_commands_total",
		"llm_d_epp_redis_syncer_valkey_command_duration_seconds",
		"llm_d_epp_redis_syncer_valkey_up",
		"llm_d_epp_redis_syncer_active_peers",
		"llm_d_epp_redis_syncer_peer_index_entries",
		"llm_d_epp_redis_syncer_last_successful_sync_timestamp_seconds",
		"llm_d_epp_redis_syncer_stream_budget_bytes_per_second",
	})
}

func TestFactoryPublishesStreamBudgetGauge(t *testing.T) {
	server := miniredis.RunT(t)
	streamBudgetBytesPerSecond.Reset()
	plugin, err := RedisSyncerFactory("redis", json.NewDecoder(strings.NewReader(fmt.Sprintf(
		`{"address": %q, "maxBytesPerSecond": 2048}`, server.Addr(),
	))), nil)
	require.NoError(t, err)
	syncer := plugin.(*RedisSyncer)
	t.Cleanup(func() { require.NoError(t, syncer.client.Close()) })

	for _, direction := range []string{directionPublish, directionRead} {
		require.Equal(t, 2048., testutil.ToFloat64(streamBudgetBytesPerSecond.WithLabelValues(direction)))
	}
}

func TestFactoryReportsValkeyDownUntilACommandSucceeds(t *testing.T) {
	server := miniredis.RunT(t)
	valkeyUp.Set(1)
	plugin, err := RedisSyncerFactory("redis", json.NewDecoder(strings.NewReader(fmt.Sprintf(
		`{"address": %q}`, server.Addr(),
	))), nil)
	require.NoError(t, err)
	syncer := plugin.(*RedisSyncer)
	t.Cleanup(func() { require.NoError(t, syncer.client.Close()) })
	require.Zero(t, testutil.ToFloat64(valkeyUp), "construction does not reach Valkey")

	require.NoError(t, syncer.client.Ping(t.Context()).Err())
	require.Equal(t, 1., testutil.ToFloat64(valkeyUp), "the factory client counts its own commands")
}

func TestValkeyMetricsHookCountsCommandsAndOutcomes(t *testing.T) {
	valkeyCommands.Reset()
	valkeyCommandDuration.Reset()
	hook := valkeyMetricsHook{}
	transportErr := errors.New("dial tcp: connection refused")
	next := func(_ context.Context, cmd goredis.Cmder) error { return cmd.Err() }

	require.NoError(t, hook.ProcessHook(next)(context.Background(), goredis.NewCmd(context.Background(), "hset")))
	require.Error(t, hook.ProcessHook(next)(context.Background(), withError(goredis.NewCmd(context.Background(), "hgetall"), transportErr)))

	require.Equal(t, 1., testutil.ToFloat64(valkeyCommands.WithLabelValues("HSET", resultSuccess)))
	require.Equal(t, 1., testutil.ToFloat64(valkeyCommands.WithLabelValues("HGETALL", resultError)))
	require.Equal(t, 2, testutil.CollectAndCount(valkeyCommandDuration))
	require.Positive(t, histogramCount(t, valkeyCommandDuration.WithLabelValues("HSET")))
	require.Zero(t, testutil.ToFloat64(valkeyUp))

	// A server-side error still proves the server answered.
	observeValkeyCommand("hset", time.Now(), goredis.Nil)
	require.Equal(t, 1., testutil.ToFloat64(valkeyUp))
}

func TestValkeyMetricsHookLeavesReachabilityOnCancel(t *testing.T) {
	valkeyUp.Set(1)

	observeValkeyCommand("hset", time.Now(), fmt.Errorf("hset: %w", context.Canceled))

	require.Equal(t, 1., testutil.ToFloat64(valkeyUp), "a cancelled command says nothing about reachability")
}

func TestValkeyMetricsHookCountsEveryPipelineCommand(t *testing.T) {
	valkeyCommands.Reset()
	hook := valkeyMetricsHook{}
	cmds := []goredis.Cmder{
		goredis.NewCmd(context.Background(), "hset"),
		goredis.NewCmd(context.Background(), "pexpire"),
	}
	next := func(_ context.Context, _ []goredis.Cmder) error { return nil }

	require.NoError(t, hook.ProcessPipelineHook(next)(context.Background(), cmds))

	require.Equal(t, 1., testutil.ToFloat64(valkeyCommands.WithLabelValues("HSET", resultSuccess)))
	require.Equal(t, 1., testutil.ToFloat64(valkeyCommands.WithLabelValues("PEXPIRE", resultSuccess)))
}

func TestSetUpdatesSuccessTimestamp(t *testing.T) {
	server := miniredis.RunT(t)
	syncer, _ := newTestSyncer(t, server, testReplicaID)
	lastSuccessfulSync.Reset()

	before := time.Now()
	require.NoError(t, syncer.set(context.Background(), testStateKey, testEndpointID, 4, sumInts))

	got := testutil.ToFloat64(lastSuccessfulSync.WithLabelValues(string(testStateKey)))
	require.GreaterOrEqual(t, got, float64(before.UnixNano())/1e9)
	require.LessOrEqual(t, got, float64(time.Now().UnixNano())/1e9)
}

func TestFailedSetLeavesSuccessTimestamp(t *testing.T) {
	recorder := &commandRecorder{err: errors.New("redis unavailable")}
	client := goredis.NewClient(&goredis.Options{})
	client.AddHook(recorder)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	syncer := &RedisSyncer{replicaID: testReplicaID, client: client, stateTTL: testStateTTL}
	lastSuccessfulSync.Reset()

	require.Error(t, syncer.set(context.Background(), testStateKey, testEndpointID, 4, sumInts))

	require.Equal(t, 0., testutil.ToFloat64(lastSuccessfulSync.WithLabelValues(string(testStateKey))))
}

func TestPeerGaugesCountDistinctPeersAcrossEndpoints(t *testing.T) {
	server := miniredis.RunT(t)
	syncer, _ := newTestSyncer(t, server, testReplicaID)
	syncer.peerGauges = newPeerGauges()
	activePeers.Reset()
	peerIndexEntries.Reset()
	label := string(testStateKey)
	seed := func(endpointID string, replicaIDs ...string) {
		t.Helper()
		for _, replicaID := range replicaIDs {
			data, err := crossreplica.EncodeValue(7)
			require.NoError(t, err)
			server.HSet(syncer.hashKey(testStateKey, endpointID), replicaID, string(data))
		}
	}

	seed("ns/endpoint-1", "epp-b")
	require.NoError(t, syncer.set(context.Background(), testStateKey, "ns/endpoint-1", 4, sumInts))
	require.Equal(t, 1., testutil.ToFloat64(activePeers.WithLabelValues(label)))
	require.Equal(t, 1., testutil.ToFloat64(peerIndexEntries.WithLabelValues(label)))

	// The same peer on a second endpoint is still one peer, and its value counts
	// once per endpoint.
	seed("ns/endpoint-2", "epp-b")
	require.NoError(t, syncer.set(context.Background(), testStateKey, "ns/endpoint-2", 4, sumInts))
	require.Equal(t, 1., testutil.ToFloat64(activePeers.WithLabelValues(label)))
	require.Equal(t, 2., testutil.ToFloat64(peerIndexEntries.WithLabelValues(label)))

	seed("ns/endpoint-2", "epp-c")
	require.NoError(t, syncer.set(context.Background(), testStateKey, "ns/endpoint-2", 4, sumInts))
	require.Equal(t, 2., testutil.ToFloat64(activePeers.WithLabelValues(label)))
}

func TestPeerGaugesDropPeersWithTheCacheEntry(t *testing.T) {
	server := miniredis.RunT(t)
	syncer, _ := newTestSyncer(t, server, testReplicaID)
	syncer.peerGauges = newPeerGauges()
	activePeers.Reset()
	peerIndexEntries.Reset()
	label := string(testStateKey)

	seedReplica(t, server, testStateKey, "epp-b", 7)
	require.NoError(t, syncer.set(context.Background(), testStateKey, testEndpointID, 4, sumInts))
	require.Equal(t, 1., testutil.ToFloat64(activePeers.WithLabelValues(label)))

	require.NoError(t, syncer.Delete(context.Background(), testStateKey, testEndpointID))
	require.Zero(t, testutil.ToFloat64(activePeers.WithLabelValues(label)))
	require.Zero(t, testutil.ToFloat64(peerIndexEntries.WithLabelValues(label)))

	// A hash with no peer value leaves nothing behind either.
	server.HDel(syncer.hashKey(testStateKey, testEndpointID), "epp-b")
	require.NoError(t, syncer.set(context.Background(), testStateKey, testEndpointID, 4, sumInts))
	require.Zero(t, testutil.ToFloat64(activePeers.WithLabelValues(label)))
}

func TestPeerGaugesCountStreamPeerElements(t *testing.T) {
	server := miniredis.RunT(t)
	a := newStreamReplica(t, server, "epp-a", true)
	b := newStreamReplica(t, server, "epp-b", true)
	a.syncer.peerGauges = newPeerGauges()
	b.syncer.peerGauges = newPeerGauges()
	activePeers.Reset()
	peerIndexEntries.Reset()
	label := string(streamStateKey)

	a.append([]uint64{1}, []uint64{2}, []uint64{3})
	a.publish(t)
	b.publish(t)

	require.Equal(t, 1., testutil.ToFloat64(activePeers.WithLabelValues(label)))
	require.Equal(t, 3., testutil.ToFloat64(peerIndexEntries.WithLabelValues(label)))

	a.evict(1)
	a.publish(t)
	b.publish(t)
	require.Equal(t, 2., testutil.ToFloat64(peerIndexEntries.WithLabelValues(label)),
		"an evicted prompt leaves the peer entry count")
}

func TestPeerGaugesStayBalancedAcrossRepeatedPublications(t *testing.T) {
	server := miniredis.RunT(t)
	a := newStreamReplica(t, server, "epp-a", true)
	b := newStreamReplica(t, server, "epp-b", true)
	a.syncer.peerGauges = newPeerGauges()
	b.syncer.peerGauges = newPeerGauges()
	activePeers.Reset()
	peerIndexEntries.Reset()
	label := string(streamStateKey)

	a.append([]uint64{1}, []uint64{2})
	a.publish(t)
	for range 5 {
		b.publish(t)
	}
	require.Equal(t, 1., testutil.ToFloat64(activePeers.WithLabelValues(label)))
	require.Equal(t, 2., testutil.ToFloat64(peerIndexEntries.WithLabelValues(label)))

	// An unchanged peer window keeps the cached aggregate and the counts.
	a.publish(t)
	b.publish(t)
	require.Equal(t, 2., testutil.ToFloat64(peerIndexEntries.WithLabelValues(label)))

	require.NoError(t, b.syncer.Delete(context.Background(), streamStateKey, testEndpointID))
	require.Zero(t, testutil.ToFloat64(activePeers.WithLabelValues(label)))
	require.Zero(t, testutil.ToFloat64(peerIndexEntries.WithLabelValues(label)))
}

// withError returns cmd after making it report err, which is how a pipeline's
// commands carry their individual outcomes.
func withError(cmd *goredis.Cmd, err error) *goredis.Cmd {
	cmd.SetErr(err)
	return cmd
}

// histogramCount returns the observation count of h.
func histogramCount(t *testing.T, h prometheus.Observer) uint64 {
	t.Helper()
	metric := &dto.Metric{}
	collector, ok := h.(prometheus.Metric)
	require.True(t, ok)
	require.NoError(t, collector.Write(metric))
	return metric.GetHistogram().GetSampleCount()
}
