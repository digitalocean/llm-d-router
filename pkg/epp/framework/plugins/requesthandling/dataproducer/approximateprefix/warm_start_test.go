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

package approximateprefix

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
)

func TestMergeWarmStartFoldsPeerStateIntoTheIndex(t *testing.T) {
	syncOn := true
	cfg := defaultConfig
	cfg.SyncCrossReplicaState = &syncOn
	cfg.AutoTune = false
	cfg.BlockSizeTokens = 64
	cfg.LRUCapacityPerServer = 8
	producer, err := newDataProducer(context.Background(), ApproxPrefixCachePluginType, cfg, testHandleWithSyncer())
	require.NoError(t, err)

	podID := k8stypes.NamespacedName{Namespace: "ns", Name: "decode-0"}
	endpoint := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: podID}, fwkdl.NewMetrics())
	// Extract registers the endpoint and installs a local-only placeholder for the
	// sync attribute, which is what the initial event sets do at startup.
	require.NoError(t, producer.Extract(context.Background(), fwkdl.EndpointEvent{
		Type:     fwkdl.EventAddOrUpdate,
		Endpoint: endpoint,
	}))
	// The cross-replica publisher then replaces the placeholder with the
	// syncer-backed attribute. A warm start has already filled the syncer's cache,
	// so Get returns peer state before any request has run.
	endpoint.GetAttributes().Put(producer.syncHashDK, &fwkdl.DynamicAttribute{
		Get: func() fwkdl.Cloneable {
			return &attrprefix.PrefixHashSet{Prompts: [][]uint64{{1, 2, 3}}}
		},
	})

	candidates := []ServerID{ServerID(podID)}
	require.Equal(t, []int{0}, producer.indexerInst.MatchLongestPrefix([]blockHash{1, 2, 3}, candidates))

	producer.MergeWarmStart(context.Background())

	require.Equal(t, []int{3}, producer.indexerInst.MatchLongestPrefix([]blockHash{1, 2, 3}, candidates),
		"peer prefix state is in the index before the first request")
}

func TestMergeWarmStartIsANoopWithoutASyncerOrEndpoints(t *testing.T) {
	syncOn := true
	cfg := defaultConfig
	cfg.SyncCrossReplicaState = &syncOn
	cfg.AutoTune = false
	producer, err := newDataProducer(context.Background(), ApproxPrefixCachePluginType, cfg, testHandleWithSyncer())
	require.NoError(t, err)

	// No endpoint registered yet.
	producer.MergeWarmStart(context.Background())

	// A producer without a syncer does nothing even with endpoints registered.
	noSyncer, err := newDataProducer(context.Background(), ApproxPrefixCachePluginType, cfg,
		plugin.NewEppHandle(context.Background(), nil, plugin.WithMetricsRecorder(prometheus.NewRegistry())))
	require.NoError(t, err)
	require.False(t, noSyncer.syncerActive())
	noSyncer.MergeWarmStart(context.Background())
}
