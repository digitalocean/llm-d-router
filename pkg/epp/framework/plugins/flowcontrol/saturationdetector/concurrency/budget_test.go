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

package concurrency

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
)

// fakeBudgetSource is a budgetSource whose count and peer freshness the test
// drives.
type fakeBudgetSource struct {
	replicas atomic.Int64
	known    atomic.Bool
	fresh    atomic.Bool
}

func (s *fakeBudgetSource) ActiveReplicas() (int, bool) {
	return int(s.replicas.Load()), s.known.Load()
}

func (s *fakeBudgetSource) peerLoadFresh() bool { return s.fresh.Load() }

// report makes the source report count replicas with the given peer freshness.
func (s *fakeBudgetSource) report(count int, fresh bool) *fakeBudgetSource {
	s.replicas.Store(int64(count))
	s.known.Store(true)
	s.fresh.Store(fresh)
	return s
}

// loseMakes the source lose its count, as a syncer with no observation does.
func (s *fakeBudgetSource) loseCount() {
	s.replicas.Store(0)
	s.known.Store(false)
	s.fresh.Store(false)
}

// staleMakes peer values stale while keeping the last count, as an unreachable
// store does.
func (s *fakeBudgetSource) stale() {
	s.fresh.Store(false)
}

// dynamicTestConfig returns a config for a deployment-wide budget of 64
// requests and 352000 tokens in the given mode.
func dynamicTestConfig(mode budgetMode, oversubscription float64, expectedReplicas int) config {
	return config{
		mode:                   modeRequests,
		maxConcurrency:         64,
		maxTokenConcurrency:    352000,
		budgetMode:             mode,
		budgetOversubscription: oversubscription,
		expectedReplicas:       expectedReplicas,
	}
}

// newBudgetDetector builds a detector with a budget whose scope is already
// resolved. peerLoad is the in-flight load scope the test wants.
func newBudgetDetector(t *testing.T, name string, cfg config, source budgetSource, peerLoad peerLoadScope) *detector {
	t.Helper()
	detector := newDetector(name, cfg, logr.Discard())
	require.NotNil(t, detector.budget)
	detector.budget.setSource(source)
	detector.budget.setPeerLoadScope(peerLoad)
	return detector
}

func TestEffectiveLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		global           int64
		replicas         int
		oversubscription float64
		want             int64
	}{
		{name: "one_replica", global: 64, replicas: 1, oversubscription: 1, want: 64},
		{name: "two_replicas", global: 64, replicas: 2, oversubscription: 1, want: 32},
		{name: "three_replicas_rounds_up", global: 64, replicas: 3, oversubscription: 1, want: 22},
		{name: "tokens_two_replicas", global: 352000, replicas: 2, oversubscription: 1, want: 176000},
		{name: "tokens_three_replicas_rounds_up", global: 352000, replicas: 3, oversubscription: 1, want: 117334},
		{name: "oversubscription_rounds_up", global: 64, replicas: 3, oversubscription: 1.1, want: 24},
		{name: "oversubscription_exact", global: 64, replicas: 2, oversubscription: 1.5, want: 48},
		{name: "oversubscription_below_one", global: 64, replicas: 2, oversubscription: 0.5, want: 16},
		{name: "never_below_one", global: 1, replicas: 4, oversubscription: 0.5, want: 1},
		{name: "zero_replicas_keeps_global", global: 64, replicas: 0, oversubscription: 1, want: 64},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, effectiveLimit(tc.global, tc.replicas, tc.oversubscription))
		})
	}
}

// TestBudget_Always verifies `always` divides the limits for every replica
// count, for both dimensions.
func TestBudget_Always(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		replicas     int
		wantRequests int64
		wantTokens   int64
	}{
		{name: "one_replica", replicas: 1, wantRequests: 64, wantTokens: 352000},
		{name: "two_replicas", replicas: 2, wantRequests: 32, wantTokens: 176000},
		{name: "three_replicas", replicas: 3, wantRequests: 22, wantTokens: 117334},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			detector := newBudgetDetector(t, "budget-always-"+tc.name, dynamicTestConfig(budgetAlways, 1.0, 0),
				(&fakeBudgetSource{}).report(tc.replicas, false), peerLoadLocal)

			requests, tokens := detector.limits()
			require.Equal(t, tc.wantRequests, requests)
			require.Equal(t, tc.wantTokens, tokens)
		})
	}
}

// TestBudget_OnPeerLoss_FreshPeersKeepFullLimit verifies the production mode
// leaves the full deployment-wide limit in force while peer load is fresh: the
// aggregate already covers every replica, so dividing as well would halve
// capacity.
func TestBudget_OnPeerLoss_FreshPeersKeepFullLimit(t *testing.T) {
	t.Parallel()

	source := (&fakeBudgetSource{}).report(2, true)
	detector := newBudgetDetector(t, "budget-peer-fresh", dynamicTestConfig(budgetOnPeerLoss, 1.0, 0), source, peerLoadSynced)

	requests, tokens := detector.limits()
	require.Equal(t, int64(64), requests, "fresh peer load covers the deployment, so the full limit applies")
	require.Equal(t, int64(352000), tokens)
	require.Equal(t, 0.0, promtestutil.ToFloat64(budgetDivided.WithLabelValues("budget-peer-fresh")))
	require.Equal(t, 1.0, promtestutil.ToFloat64(budgetReplicas.WithLabelValues("budget-peer-fresh")),
		"the replica gauge reports 1 when nothing is divided")
}

// TestBudget_OnPeerLoss_StoreDownDividesByLastCount verifies an unreachable
// store divides the limit by the last known replica count, and recovery
// restores the full limit.
func TestBudget_OnPeerLoss_StoreDownDividesByLastCount(t *testing.T) {
	t.Parallel()

	source := (&fakeBudgetSource{}).report(2, true)
	detector := newBudgetDetector(t, "budget-peer-loss", dynamicTestConfig(budgetOnPeerLoss, 1.0, 0), source, peerLoadSynced)

	requests, _ := detector.limits()
	require.Equal(t, int64(64), requests)

	// The store becomes unreachable: peer values are stale and this replica's
	// load no longer counts the peer, so it covers the deployment alone.
	source.stale()
	requests, tokens := detector.limits()
	require.Equal(t, int64(32), requests, "the limit is divided by the last known replica count")
	require.Equal(t, int64(176000), tokens)
	require.Equal(t, 1.0, promtestutil.ToFloat64(budgetDivided.WithLabelValues("budget-peer-loss")))
	require.Equal(t, 2.0, promtestutil.ToFloat64(budgetReplicas.WithLabelValues("budget-peer-loss")))

	// The store recovers: peer load is fresh again and the full limit returns.
	source.report(2, true)
	requests, tokens = detector.limits()
	require.Equal(t, int64(64), requests, "recovery restores the full limit")
	require.Equal(t, int64(352000), tokens)
	require.Equal(t, 0.0, promtestutil.ToFloat64(budgetDivided.WithLabelValues("budget-peer-loss")))
}

// TestBudget_OnPeerLoss_SingleReplica verifies a single replica always enforces
// the full limit, fresh peers or not.
func TestBudget_OnPeerLoss_SingleReplica(t *testing.T) {
	t.Parallel()

	source := (&fakeBudgetSource{}).report(1, true)
	detector := newBudgetDetector(t, "budget-peer-single", dynamicTestConfig(budgetOnPeerLoss, 1.0, 0), source, peerLoadSynced)
	requests, tokens := detector.limits()
	require.Equal(t, int64(64), requests)
	require.Equal(t, int64(352000), tokens)

	source.stale()
	requests, tokens = detector.limits()
	require.Equal(t, int64(64), requests, "dividing by one replica is the full limit")
	require.Equal(t, int64(352000), tokens)
}

// TestBudget_OnPeerLoss_LocalLoadDivides verifies that against load which
// excludes peers the mode divides unconditionally: there are no peer values to
// lose, so the deployment total depends on the division.
func TestBudget_OnPeerLoss_LocalLoadDivides(t *testing.T) {
	t.Parallel()

	source := (&fakeBudgetSource{}).report(2, false)
	detector := newBudgetDetector(t, "budget-peer-local", dynamicTestConfig(budgetOnPeerLoss, 1.0, 0), source, peerLoadLocal)

	requests, tokens := detector.limits()
	require.Equal(t, int64(32), requests)
	require.Equal(t, int64(176000), tokens)
	require.Equal(t, 1.0, promtestutil.ToFloat64(budgetDivided.WithLabelValues("budget-peer-local")))
}

// TestBudget_OnPeerLoss_UnknownScopeStaysDivided verifies the conservative
// default before the in-flight producer is on the handle.
func TestBudget_OnPeerLoss_UnknownScopeStaysDivided(t *testing.T) {
	t.Parallel()

	source := (&fakeBudgetSource{}).report(2, true)
	detector := newBudgetDetector(t, "budget-peer-unknown", dynamicTestConfig(budgetOnPeerLoss, 1.0, 0), source, peerLoadUnknown)

	requests, _ := detector.limits()
	require.Equal(t, int64(32), requests, "a partially wired detector keeps the conservative share")
}

// TestBudget_UnreachableStoreKeepsLastBudget verifies the count survives a
// source that reports none, so the share does not widen to the global limit.
func TestBudget_UnreachableStoreKeepsLastBudget(t *testing.T) {
	t.Parallel()

	source := (&fakeBudgetSource{}).report(2, false)
	detector := newBudgetDetector(t, "budget-unreachable", dynamicTestConfig(budgetAlways, 1.0, 0), source, peerLoadLocal)

	requests, tokens := detector.limits()
	require.Equal(t, int64(32), requests)
	require.Equal(t, int64(176000), tokens)

	source.loseCount()
	requests, tokens = detector.limits()
	require.Equal(t, int64(32), requests, "a lost count must not widen the budget")
	require.Equal(t, int64(176000), tokens)

	source.report(3, false)
	requests, tokens = detector.limits()
	require.Equal(t, int64(22), requests)
	require.Equal(t, int64(117334), tokens)
}

// TestBudget_PeerLeavesAndJoins verifies the share follows the replica count.
func TestBudget_PeerLeavesAndJoins(t *testing.T) {
	t.Parallel()

	source := (&fakeBudgetSource{}).report(2, false)
	detector := newBudgetDetector(t, "budget-leaves", dynamicTestConfig(budgetAlways, 1.0, 0), source, peerLoadLocal)

	requests, tokens := detector.limits()
	require.Equal(t, int64(32), requests, "two replicas halve the request budget")
	require.Equal(t, int64(176000), tokens, "two replicas halve the token budget")

	// The peer's heartbeat expires and the count drops.
	source.report(1, false)
	requests, tokens = detector.limits()
	require.Equal(t, int64(64), requests, "the survivor takes over the departed peer's share")
	require.Equal(t, int64(352000), tokens)

	source.report(2, false)
	requests, tokens = detector.limits()
	require.Equal(t, int64(32), requests)
	require.Equal(t, int64(176000), tokens)
}

// TestBudget_ColdStart verifies the limits before any replica count is known.
func TestBudget_ColdStart(t *testing.T) {
	t.Parallel()

	t.Run("expected replicas divide the budget", func(t *testing.T) {
		t.Parallel()
		source := &fakeBudgetSource{}
		detector := newBudgetDetector(t, "budget-cold-start-expected", dynamicTestConfig(budgetOnPeerLoss, 1.0, 2), source, peerLoadSynced)

		requests, tokens := detector.limits()
		require.Equal(t, int64(32), requests)
		require.Equal(t, int64(176000), tokens)

		source.report(3, true)
		requests, tokens = detector.limits()
		require.Equal(t, int64(64), requests, "fresh peer load restores the full limit")
		require.Equal(t, int64(352000), tokens)
	})

	t.Run("no replica count keeps the global limit", func(t *testing.T) {
		t.Parallel()
		detector := newBudgetDetector(t, "budget-cold-start-unknown", dynamicTestConfig(budgetAlways, 1.0, 0),
			&fakeBudgetSource{}, peerLoadLocal)

		requests, tokens := detector.limits()
		require.Equal(t, int64(64), requests)
		require.Equal(t, int64(352000), tokens)
	})

	t.Run("no source keeps the global limit", func(t *testing.T) {
		t.Parallel()
		detector := newDetector("budget-cold-start-no-syncer", dynamicTestConfig(budgetAlways, 1.0, 0), logr.Discard())
		require.NotNil(t, detector.budget)

		requests, tokens := detector.limits()
		require.Equal(t, int64(64), requests)
		require.Equal(t, int64(352000), tokens)
	})
}

// TestBudget_Oversubscription verifies the share is scaled and rounded up.
func TestBudget_Oversubscription(t *testing.T) {
	t.Parallel()

	detector := newBudgetDetector(t, "budget-oversubscription", dynamicTestConfig(budgetAlways, 1.1, 0),
		(&fakeBudgetSource{}).report(3, false), peerLoadLocal)

	requests, tokens := detector.limits()
	require.Equal(t, int64(24), requests, "ceil(64 / 3 * 1.1) = 24")
	require.Equal(t, int64(129067), tokens, "ceil(352000 / 3 * 1.1) = 129067")
}

// TestBudget_OffKeepsGlobalLimits verifies that without division the detector
// enforces exactly the configured limits.
func TestBudget_OffKeepsGlobalLimits(t *testing.T) {
	t.Parallel()

	cfg := config{mode: modeRequests, maxConcurrency: 10, maxTokenConcurrency: 100}
	detector := newDetector("budget-off", cfg, logr.Discard())
	require.Nil(t, detector.budget, "the off mode never divides, so no budget is built")

	requests, tokens := detector.limits()
	require.Equal(t, int64(10), requests)
	require.Equal(t, int64(100), tokens)

	// Saturation and filtering are those of a detector that never had a budget.
	ctx := context.Background()
	reg := newLocalRegistry()
	driveLoad(ctx, reg, detector, "endpoint-a", 5)
	saturation := detector.Saturation(ctx, []datalayer.Endpoint{newFakeEndpoint(reg, "endpoint-a")})
	require.InDelta(t, 0.5, saturation, 1e-9)

	kept := detector.Filter(ctx, nil, []fwksched.Endpoint{newStubSchedulingEndpoint(reg, "endpoint-a")})
	require.Len(t, kept, 1)
}

// TestBudget_UnsafeAlwaysFallsBackToFullLimit verifies the runtime fallback for
// `always` against load that already includes peers.
func TestBudget_UnsafeAlwaysFallsBackToFullLimit(t *testing.T) {
	t.Parallel()

	source := (&fakeBudgetSource{}).report(2, true)
	detector := newBudgetDetector(t, "budget-unsafe-always", dynamicTestConfig(budgetAlways, 1.0, 0), source, peerLoadSynced)

	// The resolver finds the producer syncing peers while a syncer is
	// configured, which makes `always` invalid.
	detector.resolveBudgetWith(t, true, true)

	requests, tokens := detector.limits()
	require.Equal(t, int64(64), requests, "the unsafe mode falls back to the full limit")
	require.Equal(t, int64(352000), tokens)
	require.Equal(t, 0.0, promtestutil.ToFloat64(budgetDivided.WithLabelValues("budget-unsafe-always")))
}

// TestBudget_ResolveScope verifies the scope the resolver derives from the
// in-flight producer's cross-replica configuration.
func TestBudget_ResolveScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		mode        budgetMode
		syncsPeers  bool
		haveSyncer  bool
		wantScope   peerLoadScope
		wantDisable bool
	}{
		{name: "onPeerLoss with synced load requires freshness", mode: budgetOnPeerLoss, syncsPeers: true, haveSyncer: true, wantScope: peerLoadSynced},
		{name: "onPeerLoss with local load divides unconditionally", mode: budgetOnPeerLoss, syncsPeers: false, haveSyncer: true, wantScope: peerLoadLocal},
		{name: "always with local load divides", mode: budgetAlways, syncsPeers: false, haveSyncer: true, wantScope: peerLoadLocal},
		{name: "always with synced load and no syncer divides", mode: budgetAlways, syncsPeers: true, haveSyncer: false, wantScope: peerLoadLocal},
		{name: "always with synced load and a syncer is disabled", mode: budgetAlways, syncsPeers: true, haveSyncer: true, wantScope: peerLoadUnknown, wantDisable: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			detector := newBudgetDetector(t, "budget-resolve", dynamicTestConfig(tc.mode, 1.0, 0),
				(&fakeBudgetSource{}).report(2, false), peerLoadUnknown)
			detector.resolveBudgetWith(t, tc.syncsPeers, tc.haveSyncer)

			require.Equal(t, tc.wantScope, peerLoadScope(detector.budget.scope.Load()))
			require.Equal(t, tc.wantDisable, detector.budget.disabled.Load())

			requests, _ := detector.limits()
			if tc.wantDisable {
				require.Equal(t, int64(64), requests)
			} else {
				require.Equal(t, int64(32), requests)
			}
		})
	}
}

// TestBudget_AppliesToSaturationAndFilter verifies the effective limits drive
// both the pool saturation signal and the per-endpoint filter.
func TestBudget_AppliesToSaturationAndFilter(t *testing.T) {
	t.Parallel()

	detector := newBudgetDetector(t, "budget-applies", dynamicTestConfig(budgetAlways, 1.0, 0),
		(&fakeBudgetSource{}).report(2, false), peerLoadLocal)

	ctx := context.Background()
	reg := newLocalRegistry()
	// 32 in-flight requests is full for a replica sharing a 64-request budget.
	driveLoad(ctx, reg, detector, "endpoint-a", 32)
	saturation := detector.Saturation(ctx, []datalayer.Endpoint{newFakeEndpoint(reg, "endpoint-a")})
	require.InDelta(t, 1.0, saturation, 1e-9, "the per-replica share is the capacity, not the global limit")

	kept := detector.Filter(ctx, nil, []fwksched.Endpoint{
		newStubSchedulingEndpoint(reg, "endpoint-a"),
		newStubSchedulingEndpoint(reg, cleanEndpoint),
	})
	require.Len(t, kept, 1)
	require.Equal(t, cleanEndpoint, kept[0].GetMetadata().ID.Name)
}

// TestBudget_TokenMode verifies the token limit is divided as well.
func TestBudget_TokenMode(t *testing.T) {
	t.Parallel()

	cfg := dynamicTestConfig(budgetAlways, 1.0, 0)
	cfg.mode = modeTokens
	detector := newBudgetDetector(t, "budget-tokens", cfg, (&fakeBudgetSource{}).report(2, false), peerLoadLocal)

	ctx := context.Background()
	reg := newLocalRegistry()
	reg.update(fullEndpointName("endpoint-a"), func(load *attrconcurrency.InFlightLoad) { load.Tokens = 88000 })
	saturation := detector.Saturation(ctx, []datalayer.Endpoint{newFakeEndpoint(reg, "endpoint-a")})
	require.InDelta(t, 0.5, saturation, 1e-9, "88000 of a 176000-token share")
}

// TestBudget_HandleSource verifies the count and freshness are read from the
// cross-replica syncer on the plugin handle.
func TestBudget_HandleSource(t *testing.T) {
	t.Parallel()

	// Without a syncer on the handle neither is available.
	source := &handleBudgetSource{handle: fwkplugin.NewEppHandle(t.Context(), nil)}
	_, ok := source.ActiveReplicas()
	require.False(t, ok)
	require.False(t, source.peerLoadFresh())

	// A syncer that counts replicas and reports freshness supplies both.
	handle := fwkplugin.NewEppHandle(t.Context(), nil)
	syncer := &fakeSyncer{source: (&fakeBudgetSource{}).report(4, true)}
	handle.SetCrossReplicaSyncer(syncer)

	detector := newDetector("budget-handle-syncer", dynamicTestConfig(budgetOnPeerLoss, 1.0, 0), logr.Discard())
	detector.budget.setSource(&handleBudgetSource{handle: handle})
	detector.budget.setPeerLoadScope(peerLoadSynced)

	requests, tokens := detector.limits()
	require.Equal(t, int64(64), requests, "fresh peer load keeps the full limit")
	require.Equal(t, int64(352000), tokens)

	syncer.source.stale()
	requests, tokens = detector.limits()
	require.Equal(t, int64(16), requests, "stale peer load divides by the syncer's count")
	require.Equal(t, int64(88000), tokens)
}

// TestBudget_ConcurrentReads verifies concurrent evaluation is race-free and
// always observes limits from a single decision.
func TestBudget_ConcurrentReads(t *testing.T) {
	t.Parallel()

	source := (&fakeBudgetSource{}).report(2, true)
	detector := newBudgetDetector(t, "budget-concurrent", dynamicTestConfig(budgetOnPeerLoss, 1.0, 0), source, peerLoadSynced)

	valid := map[[2]int64]bool{
		{64, 352000}: true, // fresh peers: the full limit
		{32, 176000}: true, // divided by two
		{22, 117334}: true, // divided by three
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 50 {
			source.report(2, i%2 == 0)
			source.report(3, i%2 == 1)
		}
	}()

	for range 1000 {
		requests, tokens := detector.limits()
		require.True(t, valid[[2]int64{requests, tokens}],
			"limits %d/%d came from mixed decisions", requests, tokens)
	}
	<-done
}

// TestBudgetMetrics_Registration verifies the metrics register once per
// registry, so a second detector instance with the same recorder is a no-op.
func TestBudgetMetrics_Registration(t *testing.T) {
	t.Parallel()

	recorder := prometheus.NewRegistry()
	require.NoError(t, registerBudgetMetrics(recorder))
	require.NoError(t, registerBudgetMetrics(recorder), "a second detector re-registers the shared vectors")
	require.Error(t, registerBudgetMetrics(nil))
}

// TestBudgetConfig_Validation verifies the new parameters are defaulted and
// validated.
func TestBudgetConfig_Validation(t *testing.T) {
	t.Parallel()

	defaults, err := buildConfig(&apiConfig{})
	require.NoError(t, err)
	require.Equal(t, budgetOff, defaults.budgetMode, "division is off by default")
	require.False(t, defaults.dividesBudget())
	require.Equal(t, 1.0, defaults.budgetOversubscription)
	require.Zero(t, defaults.expectedReplicas)

	for _, tc := range []struct {
		name    string
		params  string
		wantErr string
	}{
		{name: "zero oversubscription", params: `{"budgetOversubscription": 0}`, wantErr: "budgetOversubscription must be strictly positive"},
		{name: "negative oversubscription", params: `{"budgetOversubscription": -1}`, wantErr: "budgetOversubscription must be strictly positive"},
		{name: "negative expected replicas", params: `{"expectedReplicas": -1}`, wantErr: "expectedReplicas must be non-negative"},
		{name: "unknown mode", params: `{"dynamicBudget": "sometimes"}`, wantErr: "unsupported dynamicBudget"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cfg apiConfig
			require.NoError(t, json.Unmarshal([]byte(tc.params), &cfg))
			_, err := buildConfig(&cfg)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// TestBudgetConfig_Modes verifies the mode string, and the bool form kept for
// compatibility.
func TestBudgetConfig_Modes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		json string
		want budgetMode
	}{
		{name: "unset", json: `{}`, want: budgetOff},
		{name: "off", json: `{"dynamicBudget": "off"}`, want: budgetOff},
		{name: "always", json: `{"dynamicBudget": "always"}`, want: budgetAlways},
		{name: "onPeerLoss", json: `{"dynamicBudget": "onPeerLoss"}`, want: budgetOnPeerLoss},
		{name: "bool true is always", json: `{"dynamicBudget": true}`, want: budgetAlways},
		{name: "bool false is off", json: `{"dynamicBudget": false}`, want: budgetOff},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cfg apiConfig
			require.NoError(t, json.Unmarshal([]byte(tc.json), &cfg))
			built, err := buildConfig(&cfg)
			require.NoError(t, err)
			require.Equal(t, tc.want, built.budgetMode)
		})
	}
}

// TestBudgetConfig_RejectsUnsafeAlwaysInFactory verifies the factory wiring and
// that the mode reaches the budget.
func TestBudgetConfig_RejectsUnsafeAlwaysInFactory(t *testing.T) {
	t.Parallel()

	handle := fwkplugin.NewEppHandle(t.Context(), nil)
	plugin, err := ConcurrencyDetectorFactory("detector", fwkplugin.StrictDecoder([]byte(`{"dynamicBudget": "onPeerLoss"}`)), handle)
	require.NoError(t, err)

	detector, ok := plugin.(*detector)
	require.True(t, ok)
	require.NotNil(t, detector.budget)
	require.Equal(t, budgetOnPeerLoss, detector.budget.mode)
}

// TestBudget_ResolveScopeFromHandle verifies the resolver reads the real
// in-flight producer's cross-replica configuration from the handle, including
// the auto-created default producer.
func TestBudget_ResolveScopeFromHandle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		mode        budgetMode
		producer    *fakeInFlightProducer
		syncer      bool
		wantScope   peerLoadScope
		wantDisable bool
	}{
		{
			name: "onPeerLoss with the default synced producer", mode: budgetOnPeerLoss,
			producer: &fakeInFlightProducer{}, syncer: true, wantScope: peerLoadSynced,
		},
		{
			name: "onPeerLoss with a local producer", mode: budgetOnPeerLoss,
			producer: &fakeInFlightProducer{syncDisabled: true}, syncer: true, wantScope: peerLoadLocal,
		},
		{
			name: "always with a local producer", mode: budgetAlways,
			producer: &fakeInFlightProducer{syncDisabled: true}, syncer: true, wantScope: peerLoadLocal,
		},
		{
			name: "always with the default synced producer is disabled", mode: budgetAlways,
			producer: &fakeInFlightProducer{}, syncer: true, wantScope: peerLoadUnknown, wantDisable: true,
		},
		{
			name: "always with a synced producer and no syncer divides", mode: budgetAlways,
			producer: &fakeInFlightProducer{}, syncer: false, wantScope: peerLoadLocal,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handle := fwkplugin.NewEppHandle(t.Context(), nil)
			// The default producer name is the plugin type, which is the name an
			// unnamed instance gets.
			handle.AddPlugin("inflight-load-producer", tc.producer)
			if tc.syncer {
				handle.SetCrossReplicaSyncer(&fakeSyncer{source: (&fakeBudgetSource{}).report(2, false)})
			}

			detector := newDetector("budget-from-handle", dynamicTestConfig(tc.mode, 1.0, 0), logr.Discard())
			detector.setHandle(handle)
			detector.budget.setSource(&handleBudgetSource{handle: handle})

			// The first evaluation resolves the mode from the producer.
			require.NotPanics(t, func() { detector.limits() })
			require.Equal(t, tc.wantScope, peerLoadScope(detector.budget.scope.Load()))
			require.Equal(t, tc.wantDisable, detector.budget.disabled.Load())
		})
	}
}

// TestBudget_ResolveScopeFromHandleNamedProducer verifies a named in-flight
// producer is the one inspected.
func TestBudget_ResolveScopeFromHandleNamedProducer(t *testing.T) {
	t.Parallel()

	handle := fwkplugin.NewEppHandle(t.Context(), nil)
	// The default producer syncs peers, but this detector reads a local one.
	handle.AddPlugin("inflight-load-producer", &fakeInFlightProducer{})
	handle.AddPlugin("admission-load", &fakeInFlightProducer{syncDisabled: true})
	handle.SetCrossReplicaSyncer(&fakeSyncer{source: (&fakeBudgetSource{}).report(2, false)})

	cfg := dynamicTestConfig(budgetOnPeerLoss, 1.0, 0)
	cfg.inFlightLoadProducerName = "admission-load"
	detector := newDetector("budget-named", cfg, logr.Discard())
	detector.setHandle(handle)
	detector.budget.setSource(&handleBudgetSource{handle: handle})

	detector.limits()
	require.Equal(t, peerLoadLocal, peerLoadScope(detector.budget.scope.Load()))
}

// --- Test helpers ---

// resolveBudgetWith runs the mode resolution against an in-flight producer with
// the given cross-replica behaviour, instead of requiring a real one on the
// handle.
func (d *detector) resolveBudgetWith(t *testing.T, syncsPeers, haveSyncer bool) {
	t.Helper()
	d.budgetOnce.Do(func() {
		d.applyBudgetResolution(syncsPeers, haveSyncer)
	})
}

// fakeSyncer is a CrossReplicaSyncer that counts replicas and reports peer
// freshness.
type fakeSyncer struct {
	source *fakeBudgetSource
}

func (s *fakeSyncer) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: "fake-syncer", Name: "fake-syncer"}
}

func (s *fakeSyncer) ActiveReplicas() (int, bool) { return s.source.ActiveReplicas() }

func (s *fakeSyncer) PeerLoadFresh() bool { return s.source.peerLoadFresh() }

// fakeInFlightProducer is an in-flight load producer whose cross-replica
// configuration the detector reads.
type fakeInFlightProducer struct {
	syncDisabled bool
}

func (p *fakeInFlightProducer) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: "inflight-load-producer", Name: "inflight-load-producer"}
}

func (p *fakeInFlightProducer) CrossReplicaState() fwkdl.CrossReplicaSpec {
	return fwkdl.CrossReplicaSpec{SyncDisabled: p.syncDisabled}
}
