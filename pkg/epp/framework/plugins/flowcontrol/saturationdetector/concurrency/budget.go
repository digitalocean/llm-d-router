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
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	compbasemetrics "k8s.io/component-base/metrics"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	metricsutil "github.com/llm-d/llm-d-router/pkg/common/observability/metrics"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	eppmetrics "github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

var (
	effectiveRequestBudget = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "flow_control_effective_request_budget",
			Help: metricsutil.HelpMsgWithStability("Request limit the concurrency detector enforces on this replica. Equals maxConcurrency when "+
				"dynamicBudget is off or has not divided the limit; otherwise it is the deployment-wide maxConcurrency divided across "+
				"active EPP replicas and scaled by budgetOversubscription.", compbasemetrics.ALPHA),
		},
		[]string{"detector"},
	)

	effectiveTokenBudget = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "flow_control_effective_token_budget",
			Help: metricsutil.HelpMsgWithStability("Token limit the concurrency detector enforces on this replica. Equals maxTokenConcurrency when "+
				"dynamicBudget is off or has not divided the limit; otherwise it is the deployment-wide maxTokenConcurrency divided across "+
				"active EPP replicas and scaled by budgetOversubscription.", compbasemetrics.ALPHA),
		},
		[]string{"detector"},
	)

	budgetReplicas = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "flow_control_budget_replicas",
			Help: metricsutil.HelpMsgWithStability("Active EPP replicas the flow-control budget was divided by, counted from cross-replica "+
				"heartbeats. Reports 1 when the full deployment-wide limit is in use.", compbasemetrics.ALPHA),
		},
		[]string{"detector"},
	)

	budgetDivided = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "flow_control_budget_divided",
			Help: metricsutil.HelpMsgWithStability("Whether the flow-control limit is currently divided across replicas: 1 when the "+
				"per-replica share is enforced, 0 when the full deployment-wide limit is in use. With dynamicBudget onPeerLoss, 1 "+
				"means peer in-flight load is missing or stale and this replica is covering the deployment-wide limit alone.",
				compbasemetrics.ALPHA),
		},
		[]string{"detector"},
	)
)

// registerBudgetMetrics registers the dynamic budget gauges. The vectors are
// package-level, so a second detector instance registering them again is a
// no-op; the detector label keeps each instance's series distinct.
func registerBudgetMetrics(registerer prometheus.Registerer) error {
	if registerer == nil {
		return errors.New("dynamic budget metrics registerer is required")
	}
	for _, collector := range []prometheus.Collector{effectiveRequestBudget, effectiveTokenBudget, budgetReplicas, budgetDivided} {
		if err := registerer.Register(collector); err != nil {
			var alreadyRegistered prometheus.AlreadyRegisteredError
			if errors.As(err, &alreadyRegistered) && alreadyRegistered.ExistingCollector == collector {
				continue
			}
			return fmt.Errorf("register dynamic budget metric: %w", err)
		}
	}
	return nil
}

// recordStaticBudget reports the configured limits on the budget gauges for a
// detector that never divides them, so the gauges are populated in every mode.
func recordStaticBudget(label string, cfg config) {
	effectiveRequestBudget.WithLabelValues(label).Set(float64(cfg.maxConcurrency))
	effectiveTokenBudget.WithLabelValues(label).Set(float64(cfg.maxTokenConcurrency))
	budgetReplicas.WithLabelValues(label).Set(1)
	budgetDivided.WithLabelValues(label).Set(0)
}

// peerLoadScope records whether the in-flight load the detector reads already
// includes peer replicas.
type peerLoadScope int32

const (
	// peerLoadUnknown is the scope before the in-flight producer is on the
	// handle. Treating the load as synced keeps the limit divided, which is the
	// conservative choice while the producer does not exist yet.
	peerLoadUnknown peerLoadScope = iota
	// peerLoadLocal is load that excludes peers, so the limit must be divided
	// for the deployment total to stay within it.
	peerLoadLocal
	// peerLoadSynced is load the syncer aggregates across replicas, so it
	// already covers the deployment unless peer values are stale.
	peerLoadSynced
)

// replicaBudget decides the flow-control limits this replica enforces.
//
// With peerLoadSynced the detector reads an in-flight load the syncer
// aggregates across replicas, so fresh peer values mean the load already covers
// the deployment and the full deployment-wide limit is the right comparison;
// dividing then would halve capacity no replica can use. In modeOnPeerLoss the
// limit is divided only while peer values are missing or stale, which is
// exactly when the local load under-counts: Valkey unreachable, a peer
// aggregate expired, or a cold start. Returning to the full limit as soon as
// fresh peer data arrives is safe, because the aggregate covers the deployment
// again.
type replicaBudget struct {
	logger logr.Logger
	// label is the detector's name, the metric label value the sibling
	// flow_control_detector_saturation gauge uses for it.
	label string
	mode  budgetMode

	// global is the deployment-wide budget for each dimension.
	globalRequests int64
	globalTokens   int64
	// oversubscription scales each replica's share to tolerate uneven load.
	oversubscription float64
	// expectedReplicas is the cold-start hint for the replica count. Zero means
	// the deployment size is unknown.
	expectedReplicas int

	// scope is set once from the in-flight producer's cross-replica
	// configuration, after the producer is on the handle.
	scope atomic.Int32
	// disabled is set when the configured mode is unsafe for the load the
	// detector reads, which leaves the full limit in force.
	disabled atomic.Bool

	// source reports the replica count and, when the load is synced, whether
	// peer values are fresh. It is resolved lazily because the syncer is added
	// to the handle after plugins are instantiated.
	source atomic.Pointer[budgetSource]
	// observed is the last replica count the source reported, zero until it
	// reports one. It is kept when the source has no count, so an unreachable
	// store leaves the share where it was instead of growing to the full
	// deployment-wide budget.
	observed atomic.Int64

	missingSourceWarned atomic.Bool

	// mu guards applied, so a count change is applied and reported once.
	mu sync.Mutex
	// applied is the last computed limits, replaced as one value so a reader
	// never pairs a count with limits computed for another.
	applied atomic.Pointer[budgetLimits]
}

// budgetSource is what a replicaBudget reads: a replica count, and in
// modeOnPeerLoss whether peer values are fresh.
type budgetSource interface {
	ActiveReplicas() (int, bool)
	// peerLoadFresh reports whether peer values are recent enough to carry the
	// deployment-wide load. Sources without a freshness signal report false, so
	// the budget stays divided.
	peerLoadFresh() bool
}

// budgetLimits is one computation of the per-replica limits.
type budgetLimits struct {
	replicas int
	requests int64
	tokens   int64
	divided  bool
}

func newReplicaBudget(cfg config, label string, logger logr.Logger) *replicaBudget {
	return &replicaBudget{
		logger:           logger,
		label:            label,
		mode:             cfg.budgetMode,
		globalRequests:   cfg.maxConcurrency,
		globalTokens:     cfg.maxTokenConcurrency,
		oversubscription: cfg.budgetOversubscription,
		expectedReplicas: cfg.expectedReplicas,
	}
}

// setSource supplies the replica count. A nil source leaves the budget on its
// cold-start count.
func (b *replicaBudget) setSource(source budgetSource) {
	if source == nil {
		return
	}
	b.source.Store(&source)
}

// setPeerLoadScope records whether the in-flight load this detector reads
// includes peers. It is called once, after the producer is on the handle.
func (b *replicaBudget) setPeerLoadScope(scope peerLoadScope) {
	b.scope.Store(int32(scope))
}

// fallBackToFullLimit stops dividing, leaving the full deployment-wide limit in
// force. It is used when the configured mode is unsafe for the load read.
func (b *replicaBudget) fallBackToFullLimit() {
	b.disabled.Store(true)
}

// limits returns the request and token limits this replica enforces.
func (b *replicaBudget) limits() (requests, tokens int64) {
	decision := b.decide()
	if applied := b.applied.Load(); applied != nil &&
		applied.replicas == decision.replicas && applied.divided == decision.divided {
		return applied.requests, applied.tokens
	}
	return b.apply(decision)
}

// budgetDecision is what limits() must apply: the replica count to divide by,
// and whether to divide at all.
type budgetDecision struct {
	replicas int
	divided  bool
}

// decide resolves the current replica count and whether the limit is divided.
func (b *replicaBudget) decide() budgetDecision {
	replicas := b.replicas()
	if b.disabled.Load() {
		return budgetDecision{replicas: replicas, divided: false}
	}
	switch b.mode {
	case budgetAlways:
		return budgetDecision{replicas: replicas, divided: true}
	case budgetOnPeerLoss:
		if peerLoadScope(b.scope.Load()) == peerLoadSynced && b.peerLoadFresh() {
			// Peer load is complete, so the aggregate already covers the
			// deployment and the full limit is the right comparison.
			return budgetDecision{replicas: replicas, divided: false}
		}
		return budgetDecision{replicas: replicas, divided: true}
	default:
		return budgetDecision{replicas: replicas, divided: false}
	}
}

// peerLoadFresh reports whether the source's peer values are fresh enough to
// carry the deployment-wide load. Without a source, or with a source that has
// nothing to report, peer load is treated as unavailable so the limit stays
// divided.
func (b *replicaBudget) peerLoadFresh() bool {
	source := b.source.Load()
	return source != nil && (*source).peerLoadFresh()
}

// apply computes and records the limits for one decision.
func (b *replicaBudget) apply(decision budgetDecision) (requests, tokens int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if applied := b.applied.Load(); applied != nil &&
		applied.replicas == decision.replicas && applied.divided == decision.divided {
		// Another caller applied this decision while this one waited.
		return applied.requests, applied.tokens
	}

	requests, tokens = b.globalRequests, b.globalTokens
	if decision.divided {
		requests = effectiveLimit(b.globalRequests, decision.replicas, b.oversubscription)
		tokens = effectiveLimit(b.globalTokens, decision.replicas, b.oversubscription)
	}

	next := &budgetLimits{replicas: decision.replicas, requests: requests, tokens: tokens, divided: decision.divided}
	if previous := b.applied.Load(); previous != nil && previous.divided != next.divided {
		b.logger.V(logutil.DEFAULT).Info("Flow-control budget division changed",
			"divided", next.divided,
			"replicas", next.replicas,
			"requests", next.requests,
			"tokens", next.tokens,
			"mode", b.mode,
			"globalRequests", b.globalRequests,
			"globalTokens", b.globalTokens)
	}
	b.applied.Store(next)

	effectiveRequestBudget.WithLabelValues(b.label).Set(float64(next.requests))
	effectiveTokenBudget.WithLabelValues(b.label).Set(float64(next.tokens))
	replicasGauge := 1.0
	if next.divided {
		replicasGauge = float64(next.replicas)
	}
	budgetReplicas.WithLabelValues(b.label).Set(replicasGauge)
	budgetDivided.WithLabelValues(b.label).Set(boolGauge(next.divided))
	return next.requests, next.tokens
}

func boolGauge(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// replicas returns the count to divide the budget by: the last observed count,
// else the configured expectation, else this replica alone.
func (b *replicaBudget) replicas() int {
	if source := b.source.Load(); source != nil {
		if count, ok := (*source).ActiveReplicas(); ok && count > 0 {
			b.observed.Store(int64(count))
			return count
		}
	}
	if observed := b.observed.Load(); observed > 0 {
		// No count is available: the store is unreachable or has not reported
		// since the last observation. Keep the last share, because the full
		// deployment-wide budget would admit traffic the deployment cannot serve.
		return int(observed)
	}
	if b.expectedReplicas > 0 {
		if b.missingSourceWarned.CompareAndSwap(false, true) {
			b.logger.Info("Flow-control budget has no replica count yet; dividing by expectedReplicas",
				"replicas", b.expectedReplicas)
		}
		return b.expectedReplicas
	}
	if b.missingSourceWarned.CompareAndSwap(false, true) {
		b.logger.Info("Flow-control budget has no replica count and no expectedReplicas; " +
			"enforcing the full deployment-wide limit until one is known")
	}
	// Neither a count nor an expectation is known: keep the full
	// deployment-wide budget, which is the behavior without division.
	return 1
}

// effectiveLimit divides a deployment-wide limit by the replica count, rounding
// up so the shares never fall short of the budget through rounding.
func effectiveLimit(global int64, replicas int, oversubscription float64) int64 {
	if global <= 0 {
		return global
	}
	if replicas < 1 {
		replicas = 1
	}
	limit := math.Ceil(float64(global) / float64(replicas) * oversubscription)
	if limit < 1 {
		return 1
	}
	return int64(limit)
}

// handleBudgetSource reads the replica count, and in modeOnPeerLoss the peer
// freshness, from the cross-replica syncer configured on the plugin handle. The
// syncer is wired onto the handle after plugins are instantiated, so the
// detector cannot resolve it in its factory. The syncer is looked up until one
// that counts replicas appears, then cached: the handle field read is cheap,
// and caching only a hit means a detector evaluated before the wiring still
// finds the syncer later.
type handleBudgetSource struct {
	handle fwkplugin.Handle
	found  atomic.Pointer[fwkdl.ReplicaCounter]
	// fresh is the syncer's peer freshness, when it reports one.
	fresh atomic.Pointer[fwkdl.PeerLoadReporter]
}

func (s *handleBudgetSource) ActiveReplicas() (int, bool) {
	if found := s.found.Load(); found != nil {
		return (*found).ActiveReplicas()
	}
	counter, ok := s.handle.CrossReplicaSyncer().(fwkdl.ReplicaCounter)
	if !ok {
		return 0, false
	}
	s.found.Store(&counter)
	return counter.ActiveReplicas()
}

func (s *handleBudgetSource) peerLoadFresh() bool {
	if fresh := s.fresh.Load(); fresh != nil {
		return (*fresh).PeerLoadFresh()
	}
	reporter, ok := s.handle.CrossReplicaSyncer().(fwkdl.PeerLoadReporter)
	if !ok {
		return false
	}
	s.fresh.Store(&reporter)
	return reporter.PeerLoadFresh()
}

var _ budgetSource = (*handleBudgetSource)(nil)
var _ budgetSource = (*fixedBudgetSource)(nil)

// fixedBudgetSource is a budgetSource a test drives directly.
type fixedBudgetSource struct {
	replicas int
	known    bool
	fresh    bool
}

func (s *fixedBudgetSource) ActiveReplicas() (int, bool) { return s.replicas, s.known }

func (s *fixedBudgetSource) peerLoadFresh() bool { return s.fresh }
