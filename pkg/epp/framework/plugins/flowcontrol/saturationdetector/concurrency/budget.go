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
			Help: metricsutil.HelpMsgWithStability("Requests this replica may have in flight. Equals maxConcurrency when dynamicBudget is "+
				"off; otherwise it is the deployment-wide maxConcurrency divided across live EPP replicas and scaled by "+
				"budgetOversubscription.", compbasemetrics.ALPHA),
		},
		[]string{"detector"},
	)

	effectiveTokenBudget = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "flow_control_effective_token_budget",
			Help: metricsutil.HelpMsgWithStability("Tokens this replica may have in flight. Equals maxTokenConcurrency when dynamicBudget is "+
				"off; otherwise it is the deployment-wide maxTokenConcurrency divided across live EPP replicas and scaled by "+
				"budgetOversubscription.", compbasemetrics.ALPHA),
		},
		[]string{"detector"},
	)

	budgetReplicas = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "flow_control_budget_replicas",
			Help: metricsutil.HelpMsgWithStability("Live EPP replicas the flow-control budget was divided by, counted from cross-replica "+
				"liveness. Reports 1 when the full deployment-wide limit is in force.", compbasemetrics.ALPHA),
		},
		[]string{"detector"},
	)

	localRequestLoad = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "flow_control_local_inflight_requests",
			Help: metricsutil.HelpMsgWithStability("Requests in flight on this replica that the concurrency detector counts, summed over the "+
				"candidate endpoints of the most recent saturation evaluation. With dynamicBudget on this is this replica's own "+
				"load, excluding peers, which is what the divided request budget is compared against.", compbasemetrics.ALPHA),
		},
		[]string{"detector"},
	)

	localTokenLoad = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "flow_control_local_inflight_tokens",
			Help: metricsutil.HelpMsgWithStability("Tokens in flight on this replica that the concurrency detector counts, summed over the "+
				"candidate endpoints of the most recent saturation evaluation. With dynamicBudget on this is this replica's own "+
				"load, excluding peers, which is what the divided token budget is compared against.", compbasemetrics.ALPHA),
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
	collectors := []prometheus.Collector{
		effectiveRequestBudget, effectiveTokenBudget, budgetReplicas, localRequestLoad, localTokenLoad,
	}
	for _, collector := range collectors {
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
// detector that does not divide them, so the gauges are populated in every mode.
func recordStaticBudget(label string, cfg config) {
	effectiveRequestBudget.WithLabelValues(label).Set(float64(cfg.maxConcurrency))
	effectiveTokenBudget.WithLabelValues(label).Set(float64(cfg.maxTokenConcurrency))
	budgetReplicas.WithLabelValues(label).Set(1)
}

// observeLocalLoad records the in-flight load the last saturation evaluation
// summed, so the load compared against the divided budget is visible.
func observeLocalLoad(label string, requests, tokens int64) {
	localRequestLoad.WithLabelValues(label).Set(float64(requests))
	localTokenLoad.WithLabelValues(label).Set(float64(tokens))
}

// replicaBudget divides the deployment-wide flow-control limits across the
// replicas that are serving, so the shares add up to the capacity the
// deployment was sized for.
//
// The point is to bound cluster-wide admission even while replicas have not yet
// seen each other's admissions. Synced peer load arrives asynchronously, so a
// burst that reaches every replica at once can have each of them admit the full
// limit before any sees the others; with a shared limit divided per replica,
// each admits at most its share of its own load, so the total cannot exceed the
// limit regardless of sync lag. The cost is early shedding when load is uneven:
// a replica caps at its share even while another replica is idle. That
// trade-off is deliberate.
type replicaBudget struct {
	logger logr.Logger
	// label is the detector's name, the metric label value the sibling
	// flow_control_detector_saturation gauge uses for it.
	label string

	// global is the deployment-wide budget for each dimension.
	globalRequests int64
	globalTokens   int64
	// oversubscription scales each replica's share to tolerate uneven load.
	oversubscription float64
	// expectedReplicas is the cold-start hint for the replica count. Zero means
	// the deployment size is unknown.
	expectedReplicas int

	// source reports the live replica count. It is resolved lazily because the
	// syncer is added to the handle after plugins are instantiated.
	source atomic.Pointer[fwkdl.ReplicaCounter]
	// observed is the last replica count the source reported, zero until it
	// reports one. It is kept when the source has no count, so an unreachable
	// store leaves the share where it was instead of widening to the full
	// deployment-wide budget.
	observed atomic.Int64

	missingSourceWarned atomic.Bool

	// mu guards applied, so a count change is applied and reported once.
	mu sync.Mutex
	// applied is the last computed limits and the count they came from,
	// replaced as one value so a reader never pairs a count with limits
	// computed for another.
	applied atomic.Pointer[budgetLimits]
}

// budgetLimits is one computation of the per-replica limits.
type budgetLimits struct {
	replicas int
	requests int64
	tokens   int64
}

func newReplicaBudget(cfg config, label string, logger logr.Logger) *replicaBudget {
	return &replicaBudget{
		logger:           logger,
		label:            label,
		globalRequests:   cfg.maxConcurrency,
		globalTokens:     cfg.maxTokenConcurrency,
		oversubscription: cfg.budgetOversubscription,
		expectedReplicas: cfg.expectedReplicas,
	}
}

// setSource supplies the replica count. A nil source leaves the budget on its
// cold-start count.
func (b *replicaBudget) setSource(source fwkdl.ReplicaCounter) {
	if source == nil {
		return
	}
	b.source.Store(&source)
}

// limits returns the request and token limits this replica enforces.
func (b *replicaBudget) limits() (requests, tokens int64) {
	replicas := b.replicas()
	if applied := b.applied.Load(); applied != nil && applied.replicas == replicas {
		return applied.requests, applied.tokens
	}
	return b.apply(replicas)
}

// apply computes and records the limits for a replica count.
func (b *replicaBudget) apply(replicas int) (requests, tokens int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if applied := b.applied.Load(); applied != nil && applied.replicas == replicas {
		// Another caller applied this count while this one waited.
		return applied.requests, applied.tokens
	}

	requests = effectiveLimit(b.globalRequests, replicas, b.oversubscription)
	tokens = effectiveLimit(b.globalTokens, replicas, b.oversubscription)

	if previous := b.applied.Load(); previous != nil && previous.requests != requests {
		b.logger.V(logutil.DEFAULT).Info("Flow-control budget changed",
			"replicas", replicas,
			"requests", requests,
			"tokens", tokens,
			"globalRequests", b.globalRequests,
			"globalTokens", b.globalTokens)
	}
	b.applied.Store(&budgetLimits{replicas: replicas, requests: requests, tokens: tokens})

	effectiveRequestBudget.WithLabelValues(b.label).Set(float64(requests))
	effectiveTokenBudget.WithLabelValues(b.label).Set(float64(tokens))
	budgetReplicas.WithLabelValues(b.label).Set(float64(replicas))
	return requests, tokens
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

// handleReplicaCounter reads the replica count from the cross-replica syncer
// configured on the plugin handle. The syncer is wired onto the handle after
// plugins are instantiated, so the detector cannot resolve it in its factory.
// The syncer is looked up until one that counts replicas appears, then cached:
// the handle field read is cheap, and caching only a hit means a detector
// evaluated before the wiring still finds the syncer later.
type handleReplicaCounter struct {
	handle fwkplugin.Handle
	found  atomic.Pointer[fwkdl.ReplicaCounter]
}

func (c *handleReplicaCounter) ActiveReplicas() (int, bool) {
	if found := c.found.Load(); found != nil {
		return (*found).ActiveReplicas()
	}
	counter, ok := c.handle.CrossReplicaSyncer().(fwkdl.ReplicaCounter)
	if !ok {
		return 0, false
	}
	c.found.Store(&counter)
	return counter.ActiveReplicas()
}
