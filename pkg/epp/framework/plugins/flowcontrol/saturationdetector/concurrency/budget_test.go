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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
)

// fakeReplicaCounter is a ReplicaCounter whose count the test drives.
type fakeReplicaCounter struct {
	count atomic.Int64
	ok    atomic.Bool
}

func (c *fakeReplicaCounter) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: "fake-syncer", Name: "fake-syncer"}
}

func (c *fakeReplicaCounter) ActiveReplicas() (int, bool) { return int(c.count.Load()), c.ok.Load() }

// report makes the counter report count as a known value.
func (c *fakeReplicaCounter) report(count int) *fakeReplicaCounter {
	c.count.Store(int64(count))
	c.ok.Store(true)
	return c
}

// loseCount makes the counter report no count, as a syncer whose store is
// unreachable does.
func (c *fakeReplicaCounter) loseCount() {
	c.count.Store(0)
	c.ok.Store(false)
}

// fakeLoadReader is a LocalInFlightLoadReader whose per-endpoint counters the
// test drives. It stands in for the in-flight load producer's own trackers.
type fakeLoadReader struct {
	mu   sync.Mutex
	load map[string]*attrconcurrency.InFlightLoad
}

func newFakeLoadReader() *fakeLoadReader {
	return &fakeLoadReader{load: map[string]*attrconcurrency.InFlightLoad{}}
}

func (r *fakeLoadReader) LocalInFlightLoad(endpointID string) (requests, tokens int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if load, ok := r.load[endpointID]; ok {
		return load.Requests, load.Tokens
	}
	return 0, 0
}

func (r *fakeLoadReader) set(endpointName string, requests, tokens int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.load[fullEndpointName(endpointName)] = &attrconcurrency.InFlightLoad{Requests: requests, Tokens: tokens}
}

// dynamicTestConfig returns a config for a deployment-wide budget of 64
// requests and 352000 tokens, divided when divide is true.
func dynamicTestConfig(divide bool, oversubscription float64, expectedReplicas int) config {
	return config{
		mode:                   modeRequests,
		maxConcurrency:         64,
		maxTokenConcurrency:    352000,
		dynamicBudget:          divide,
		budgetOversubscription: oversubscription,
		expectedReplicas:       expectedReplicas,
	}
}

// newBudgetDetector builds a detector with a budget and a replica source.
func newBudgetDetector(t *testing.T, name string, cfg config, source *fakeReplicaCounter) *detector {
	t.Helper()
	detector := newDetector(name, cfg, logr.Discard())
	require.NotNil(t, detector.budget)
	detector.budget.setSource(source)
	return detector
}

// withLocalLoad supplies a local load reader directly, as the detector would
// resolve one from the in-flight load producer.
func (d *detector) withLocalLoad(reader datalayer.LocalInFlightLoadReader) *detector {
	d.localLoad.Store(&reader)
	return d
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

// TestBudget_ReplicaCounts verifies the per-replica limits for one, two and
// three live replicas.
func TestBudget_ReplicaCounts(t *testing.T) {
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
			detector := newBudgetDetector(t, "budget-"+tc.name, dynamicTestConfig(true, 1.0, 0),
				(&fakeReplicaCounter{}).report(tc.replicas))

			requests, tokens := detector.limits()
			require.Equal(t, tc.wantRequests, requests)
			require.Equal(t, tc.wantTokens, tokens)
		})
	}
}

// TestBudget_PeerLoadDoesNotCountAgainstTheShare is the core of the design: two
// replicas each admit up to their own share of their own load, and neither
// replica's share is consumed by the other's load. Counting peer load against a
// per-replica share would block cluster-wide well below the budget.
func TestBudget_PeerLoadDoesNotCountAgainstTheShare(t *testing.T) {
	t.Parallel()

	const (
		global = 10
		share  = 5
	)
	cfg := config{
		mode:                   modeRequests,
		maxConcurrency:         global,
		maxTokenConcurrency:    1000,
		dynamicBudget:          true,
		budgetOversubscription: 1,
	}

	// The syncer counts two live replicas, so each enforces half the budget.
	source := (&fakeReplicaCounter{}).report(2)

	// Replica 1 has 3 of its own requests in flight; replica 2 has 2. Together
	// those are 5, the share, but this replica's own load is only 3, so it has
	// room for 2 more.
	reader := newFakeLoadReader()
	reader.set("endpoint-a", 3, 30)
	detector := newBudgetDetector(t, "budget-peer-load", cfg, source).withLocalLoad(reader)

	requests, _ := detector.limits()
	require.Equal(t, int64(share), requests)

	ctx := context.Background()
	reg := newLocalRegistry()
	// The endpoint's installed attribute holds the cross-replica aggregate of
	// 3 + 2 = 5, which would look full against a share of 5.
	reg.update(fullEndpointName("endpoint-a"), func(load *attrconcurrency.InFlightLoad) {
		load.Requests = 3 + 2
	})

	endpoint := newFakeEndpoint(reg, "endpoint-a")
	saturation := detector.Saturation(ctx, []datalayer.Endpoint{endpoint})
	require.InDelta(t, 3.0/5.0, saturation, 1e-9,
		"saturation must use this replica's own load of 3, not the aggregate of 5")

	// The local load the budget is compared against is visible.
	require.Equal(t, 3.0, promtestutil.ToFloat64(localRequestLoad.WithLabelValues("budget-peer-load")))

	// Own load of 3 is under the share of 5, so the endpoint stays schedulable.
	kept := detector.Filter(ctx, nil, []fwksched.Endpoint{newStubSchedulingEndpoint(reg, "endpoint-a")})
	require.Len(t, kept, 1, "the aggregate 5 must not close the endpoint against a share of 5")
}

// TestBudget_EachReplicaCappedAtItsShare verifies two replicas together cannot
// exceed the deployment-wide budget even though neither sees the other's
// admissions yet. Each replica admits against its own load only, so both stop at
// the share and the total is the global limit.
func TestBudget_EachReplicaCappedAtItsShare(t *testing.T) {
	t.Parallel()

	const (
		global = 10
		share  = 5
	)
	cfg := config{
		mode:                   modeRequests,
		maxConcurrency:         global,
		maxTokenConcurrency:    1000,
		dynamicBudget:          true,
		budgetOversubscription: 1,
	}
	ctx := context.Background()

	var admitted [2]int64
	for i := range admitted {
		// This replica sees two live replicas and no peer admissions yet: the
		// peer's load is invisible to it during the burst. Its own counters are
		// the only load that counts against its share.
		reader := newFakeLoadReader()
		detector := newBudgetDetector(t, "budget-capped", cfg, (&fakeReplicaCounter{}).report(2)).withLocalLoad(reader)
		reg := newLocalRegistry()

		for {
			endpoint := newFakeEndpoint(reg, "endpoint-a")
			// Flow control releases while saturation is below 1, so stop when the
			// detector reports the share is full.
			if detector.Saturation(ctx, []datalayer.Endpoint{endpoint}) >= 1 {
				break
			}
			admitted[i]++
			own := admitted[i]
			reader.set("endpoint-a", own, 0)
			require.GreaterOrEqual(t, int64(share), own, "this replica must never exceed its share")
		}
	}

	require.Equal(t, int64(share), admitted[0], "each replica fills exactly its share")
	require.Equal(t, int64(share), admitted[1])
	require.Equal(t, int64(global), admitted[0]+admitted[1],
		"the two replicas together are capped at the deployment-wide budget")
}

// TestBudget_SurvivorWidensAfterPeerLivenessExpires verifies a peer leaving the
// live count widens the survivor's share, and the peer returning narrows it.
func TestBudget_SurvivorWidensAfterPeerLivenessExpires(t *testing.T) {
	t.Parallel()

	source := (&fakeReplicaCounter{}).report(2)
	detector := newBudgetDetector(t, "budget-leaves", dynamicTestConfig(true, 1.0, 0), source)

	requests, tokens := detector.limits()
	require.Equal(t, int64(32), requests, "two live replicas halve the request budget")
	require.Equal(t, int64(176000), tokens, "two live replicas halve the token budget")

	// The peer's liveness expires and the syncer drops it from the count.
	source.report(1)
	requests, tokens = detector.limits()
	require.Equal(t, int64(64), requests, "the survivor takes over the departed peer's share")
	require.Equal(t, int64(352000), tokens)

	// The peer returns and is counted again.
	source.report(2)
	requests, tokens = detector.limits()
	require.Equal(t, int64(32), requests)
	require.Equal(t, int64(176000), tokens)
}

// TestBudget_UnreachableStoreKeepsLastCount verifies an unreachable store leaves
// the share at the last known count: it neither widens to the full budget nor
// drops to zero.
func TestBudget_UnreachableStoreKeepsLastCount(t *testing.T) {
	t.Parallel()

	source := (&fakeReplicaCounter{}).report(2)
	detector := newBudgetDetector(t, "budget-unreachable", dynamicTestConfig(true, 1.0, 0), source)

	requests, tokens := detector.limits()
	require.Equal(t, int64(32), requests)
	require.Equal(t, int64(176000), tokens)

	source.loseCount()
	requests, tokens = detector.limits()
	require.Equal(t, int64(32), requests, "an unreachable store must not widen the budget to the full limit")
	require.NotZero(t, requests)
	require.Equal(t, int64(176000), tokens)

	// The store recovers and reports a larger fleet.
	source.report(3)
	requests, tokens = detector.limits()
	require.Equal(t, int64(22), requests)
	require.Equal(t, int64(117334), tokens)
}

// TestBudget_ColdStart verifies the limits before any replica count is known.
func TestBudget_ColdStart(t *testing.T) {
	t.Parallel()

	t.Run("expected replicas divide the budget", func(t *testing.T) {
		t.Parallel()
		source := &fakeReplicaCounter{}
		detector := newBudgetDetector(t, "budget-cold-start-expected", dynamicTestConfig(true, 1.0, 2), source)

		requests, tokens := detector.limits()
		require.Equal(t, int64(32), requests)
		require.Equal(t, int64(176000), tokens)

		// The syncer reports the real count on its first publication.
		source.report(4)
		requests, tokens = detector.limits()
		require.Equal(t, int64(16), requests)
		require.Equal(t, int64(88000), tokens)
	})

	t.Run("no replica count keeps the global limit", func(t *testing.T) {
		t.Parallel()
		detector := newBudgetDetector(t, "budget-cold-start-unknown", dynamicTestConfig(true, 1.0, 0), &fakeReplicaCounter{})

		requests, tokens := detector.limits()
		require.Equal(t, int64(64), requests)
		require.Equal(t, int64(352000), tokens)
	})

	t.Run("no source keeps the global limit", func(t *testing.T) {
		t.Parallel()
		detector := newDetector("budget-cold-start-no-syncer", dynamicTestConfig(true, 1.0, 0), logr.Discard())
		require.NotNil(t, detector.budget)

		requests, tokens := detector.limits()
		require.Equal(t, int64(64), requests)
		require.Equal(t, int64(352000), tokens)
	})
}

// TestBudget_Oversubscription verifies the share is scaled and rounded up.
func TestBudget_Oversubscription(t *testing.T) {
	t.Parallel()

	detector := newBudgetDetector(t, "budget-oversubscription", dynamicTestConfig(true, 1.1, 0),
		(&fakeReplicaCounter{}).report(3))

	requests, tokens := detector.limits()
	require.Equal(t, int64(24), requests, "ceil(64 / 3 * 1.1) = 24")
	require.Equal(t, int64(129067), tokens, "ceil(352000 / 3 * 1.1) = 129067")
}

// TestBudget_OffKeepsGlobalLimitsAndReadsTheAggregate verifies that with
// dynamicBudget off the detector enforces the configured limits and reads the
// load the attribute map holds, which may include peers.
func TestBudget_OffKeepsGlobalLimitsAndReadsTheAggregate(t *testing.T) {
	t.Parallel()

	cfg := config{mode: modeRequests, maxConcurrency: 10, maxTokenConcurrency: 100}
	detector := newDetector("budget-off", cfg, logr.Discard())
	require.Nil(t, detector.budget, "dynamicBudget is off, so no budget is built")

	// A local reader is present but must not be used.
	detector.withLocalLoad(newFakeLoadReader())

	requests, tokens := detector.limits()
	require.Equal(t, int64(10), requests)
	require.Equal(t, int64(100), tokens)

	ctx := context.Background()
	reg := newLocalRegistry()
	// The attribute holds the aggregate, and with dynamicBudget off that is what
	// the detector counts, so 5 of 10 is half full.
	reg.update(fullEndpointName("endpoint-a"), func(load *attrconcurrency.InFlightLoad) { load.Requests = 5 })
	saturation := detector.Saturation(ctx, []datalayer.Endpoint{newFakeEndpoint(reg, "endpoint-a")})
	require.InDelta(t, 0.5, saturation, 1e-9)

	kept := detector.Filter(ctx, nil, []fwksched.Endpoint{newStubSchedulingEndpoint(reg, "endpoint-a")})
	require.Len(t, kept, 1)
}

// TestBudget_OffUnaffectedByTinyShare is the counterpart to the peer-load case:
// with dynamicBudget off, load from peers is deliberately counted against the
// full limit.
func TestBudget_OffCountsPeerLoadAgainstTheFullLimit(t *testing.T) {
	t.Parallel()

	detector := newDetector("budget-off-aggregate", config{mode: modeRequests, maxConcurrency: 10}, logr.Discard())
	reader := newFakeLoadReader()
	reader.set("endpoint-a", 1, 1)
	detector.withLocalLoad(reader)

	ctx := context.Background()
	reg := newLocalRegistry()
	// This replica admits 1, but the aggregate is 4 + 4 = 8 of the 10 limit.
	reg.update(fullEndpointName("endpoint-a"), func(load *attrconcurrency.InFlightLoad) { load.Requests = 8 })

	saturation := detector.Saturation(ctx, []datalayer.Endpoint{newFakeEndpoint(reg, "endpoint-a")})
	require.InDelta(t, 0.8, saturation, 1e-9, "dynamicBudget off keeps reading the aggregate")
}

// TestBudget_AppliesToSaturationAndFilter verifies the local load drives both
// the saturation signal and the per-endpoint filter when dividing.
func TestBudget_AppliesToSaturationAndFilter(t *testing.T) {
	t.Parallel()

	detector := newBudgetDetector(t, "budget-applies", dynamicTestConfig(true, 1.0, 0),
		(&fakeReplicaCounter{}).report(2))
	reader := newFakeLoadReader()
	// 32 of this replica's own requests is full for a share of 32.
	reader.set("endpoint-a", 32, 0)
	detector.withLocalLoad(reader)

	ctx := context.Background()
	reg := newLocalRegistry()
	saturation := detector.Saturation(ctx, []datalayer.Endpoint{newFakeEndpoint(reg, "endpoint-a")})
	require.InDelta(t, 1.0, saturation, 1e-9, "the per-replica share is the capacity, not the global limit")

	kept := detector.Filter(ctx, nil, []fwksched.Endpoint{
		newStubSchedulingEndpoint(reg, "endpoint-a"),
		newStubSchedulingEndpoint(reg, cleanEndpoint),
	})
	require.Len(t, kept, 1)
	require.Equal(t, cleanEndpoint, kept[0].GetMetadata().ID.Name)
}

// TestBudget_TokenMode verifies the token limit is divided and compared against
// this replica's own tokens.
func TestBudget_TokenMode(t *testing.T) {
	t.Parallel()

	cfg := dynamicTestConfig(true, 1.0, 0)
	cfg.mode = modeTokens
	detector := newBudgetDetector(t, "budget-tokens", cfg, (&fakeReplicaCounter{}).report(2))
	reader := newFakeLoadReader()
	reader.set("endpoint-a", 0, 88000)
	detector.withLocalLoad(reader)

	ctx := context.Background()
	reg := newLocalRegistry()
	saturation := detector.Saturation(ctx, []datalayer.Endpoint{newFakeEndpoint(reg, "endpoint-a")})
	require.InDelta(t, 0.5, saturation, 1e-9, "88000 of a 176000-token share")
	require.Equal(t, 88000.0, promtestutil.ToFloat64(localTokenLoad.WithLabelValues("budget-tokens")))
}

// TestBudget_NoLocalReaderFallsBackToAttribute verifies a producer that cannot
// report local load leaves the detector on the attribute map rather than
// reading nothing.
func TestBudget_NoLocalReaderFallsBackToAttribute(t *testing.T) {
	t.Parallel()

	detector := newBudgetDetector(t, "budget-no-local", dynamicTestConfig(true, 1.0, 0),
		(&fakeReplicaCounter{}).report(2))
	detector.localLoadMissing.Store(true)

	ctx := context.Background()
	reg := newLocalRegistry()
	reg.update(fullEndpointName("endpoint-a"), func(load *attrconcurrency.InFlightLoad) { load.Requests = 16 })
	saturation := detector.Saturation(ctx, []datalayer.Endpoint{newFakeEndpoint(reg, "endpoint-a")})
	require.InDelta(t, 0.5, saturation, 1e-9, "16 of the 32-request share, read from the attribute map")
}

// TestBudget_HandleResolvesLocalReader verifies the detector resolves the
// in-flight producer by name on the handle and uses that producer's local load.
func TestBudget_HandleResolvesLocalReader(t *testing.T) {
	t.Parallel()

	handle := fwkplugin.NewEppHandle(t.Context(), nil)
	reader := newFakeLoadReader()
	reader.set("endpoint-a", 8, 0)
	handle.AddPlugin("inflight-load-producer", &namedLoadProducer{name: "inflight-load-producer", reader: reader})
	handle.SetCrossReplicaSyncer((&fakeReplicaCounter{}).report(2))

	detector := newDetector("budget-handle", dynamicTestConfig(true, 1.0, 0), logr.Discard())
	detector.setHandle(handle)
	detector.budget.setSource(&handleReplicaCounter{handle: handle})

	ctx := context.Background()
	reg := newLocalRegistry()
	// The attribute carries an aggregate that would look full on its own.
	reg.update(fullEndpointName("endpoint-a"), func(load *attrconcurrency.InFlightLoad) { load.Requests = 32 })

	saturation := detector.Saturation(ctx, []datalayer.Endpoint{newFakeEndpoint(reg, "endpoint-a")})
	require.InDelta(t, 8.0/32.0, saturation, 1e-9, "the resolved producer's local load, not the aggregate")
}

// TestBudget_HandleNamedProducer verifies the producer named in the config is
// the one resolved.
func TestBudget_HandleNamedProducer(t *testing.T) {
	t.Parallel()

	handle := fwkplugin.NewEppHandle(t.Context(), nil)
	defaultReader := newFakeLoadReader()
	defaultReader.set("endpoint-a", 32, 0)
	namedReader := newFakeLoadReader()
	namedReader.set("endpoint-a", 4, 0)
	handle.AddPlugin("inflight-load-producer", &namedLoadProducer{name: "inflight-load-producer", reader: defaultReader})
	handle.AddPlugin("admission-load", &namedLoadProducer{name: "admission-load", reader: namedReader})

	cfg := dynamicTestConfig(true, 1.0, 0)
	cfg.inFlightLoadProducerName = "admission-load"
	detector := newDetector("budget-named", cfg, logr.Discard())
	detector.setHandle(handle)
	detector.budget.setSource((&fakeReplicaCounter{}).report(2))

	ctx := context.Background()
	reg := newLocalRegistry()
	saturation := detector.Saturation(ctx, []datalayer.Endpoint{newFakeEndpoint(reg, "endpoint-a")})
	require.InDelta(t, 4.0/32.0, saturation, 1e-9, "the named producer's local load")
}

// TestBudget_ConcurrentReads verifies concurrent evaluation is race-free and
// always observes limits from a single count.
func TestBudget_ConcurrentReads(t *testing.T) {
	t.Parallel()

	source := (&fakeReplicaCounter{}).report(2)
	detector := newBudgetDetector(t, "budget-concurrent", dynamicTestConfig(true, 1.0, 0), source)

	valid := map[[2]int64]bool{
		{32, 176000}: true, // two live replicas
		{22, 117334}: true, // three live replicas
		{64, 352000}: true, // one live replica
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 50 {
			if i%2 == 0 {
				source.report(3)
			} else {
				source.report(1)
			}
		}
	}()

	for range 1000 {
		requests, tokens := detector.limits()
		require.True(t, valid[[2]int64{requests, tokens}],
			"limits %d/%d came from mixed counts", requests, tokens)
	}
	<-done
}

// TestBudget_Metrics verifies the gauges report the effective limits and the
// replica count the limits were divided by.
func TestBudget_Metrics(t *testing.T) {
	t.Parallel()

	const label = "budget-metrics"
	detector := newBudgetDetector(t, label, dynamicTestConfig(true, 1.0, 0), (&fakeReplicaCounter{}).report(2))
	detector.limits()

	require.Equal(t, 32.0, promtestutil.ToFloat64(effectiveRequestBudget.WithLabelValues(label)))
	require.Equal(t, 176000.0, promtestutil.ToFloat64(effectiveTokenBudget.WithLabelValues(label)))
	require.Equal(t, 2.0, promtestutil.ToFloat64(budgetReplicas.WithLabelValues(label)))
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

// TestBudgetConfig_Validation verifies the parameters are defaulted and
// validated.
func TestBudgetConfig_Validation(t *testing.T) {
	t.Parallel()

	defaults, err := buildConfig(&apiConfig{})
	require.NoError(t, err)
	require.False(t, defaults.dynamicBudget, "division is off by default")
	require.Equal(t, 1.0, defaults.budgetOversubscription)
	require.Zero(t, defaults.expectedReplicas)

	for _, tc := range []struct {
		name    string
		cfg     apiConfig
		wantErr string
	}{
		{name: "zero oversubscription", cfg: apiConfig{BudgetOversubscription: ptr.To(0.0)}, wantErr: "budgetOversubscription must be strictly positive"},
		{name: "negative oversubscription", cfg: apiConfig{BudgetOversubscription: ptr.To(-1.0)}, wantErr: "budgetOversubscription must be strictly positive"},
		{name: "negative expected replicas", cfg: apiConfig{ExpectedReplicas: ptr.To(-1)}, wantErr: "expectedReplicas must be non-negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := buildConfig(&tc.cfg)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}

	enabled, err := buildConfig(&apiConfig{DynamicBudget: ptr.To(true), BudgetOversubscription: ptr.To(1.1), ExpectedReplicas: ptr.To(2)})
	require.NoError(t, err)
	require.True(t, enabled.dynamicBudget)
	require.Equal(t, 1.1, enabled.budgetOversubscription)
	require.Equal(t, 2, enabled.expectedReplicas)

	disabled, err := buildConfig(&apiConfig{DynamicBudget: ptr.To(false)})
	require.NoError(t, err)
	require.False(t, disabled.dynamicBudget)
}

// namedLoadProducer is a plugin named on the handle that reports local in-flight
// load, standing in for the in-flight load producer.
type namedLoadProducer struct {
	name   string
	reader *fakeLoadReader
}

func (p *namedLoadProducer) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: "inflight-load-producer", Name: p.name}
}

func (p *namedLoadProducer) LocalInFlightLoad(endpointID string) (requests, tokens int64) {
	return p.reader.LocalInFlightLoad(endpointID)
}

var _ fwkdl.ReplicaCounter = (*fakeReplicaCounter)(nil)
var _ datalayer.LocalInFlightLoadReader = (*namedLoadProducer)(nil)
