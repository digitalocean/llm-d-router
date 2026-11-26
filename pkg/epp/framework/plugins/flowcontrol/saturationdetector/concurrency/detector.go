/*
Copyright 2025 The Kubernetes Authors.
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
// Package concurrency implements a synchronous saturation detector and scheduling filter for LLM
// routing. It consumes in-flight requests and tokens data from the Endpoint's AttributeMap
// to provide instantaneous backpressure and protect endpoints from sudden traffic bursts.
//
// For detailed architectural trade-offs and configuration, see the package README.
package concurrency

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
	inflightloadconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/dataproducer/inflightload/constants"
)

const (
	ConcurrencyDetectorType = "concurrency-detector"
)

// ConcurrencyDetectorFactory instantiates the detector plugin using the provided JSON parameters.
func ConcurrencyDetectorFactory(
	name string,
	params *json.Decoder,
	handle fwkplugin.Handle,
) (fwkplugin.Plugin, error) {
	var apiCfg apiConfig
	if params != nil {
		if err := params.Decode(&apiCfg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal concurrency detector config: %w", err)
		}
	}
	cfg, err := buildConfig(&apiCfg)
	if err != nil {
		return nil, err
	}
	detector := newDetector(name, *cfg, log.FromContext(handle.Context()))
	detector.setHandle(handle)
	if handle != nil && handle.Metrics() != nil {
		if err := registerBudgetMetrics(handle.Metrics()); err != nil {
			return nil, err
		}
	}
	if cfg.dividesBudget() && handle != nil {
		// The cross-replica syncer is added to the handle after plugins are
		// instantiated, so the replica count and the peer-load check are
		// resolved on first use. See resolveBudget.
		detector.budget.setSource(&handleBudgetSource{handle: handle})
	} else {
		recordStaticBudget(name, *cfg)
	}
	return detector, nil
}

var (
	_ fwksched.Filter                        = &detector{}
	_ flowcontrol.SaturationDetector         = &detector{}
	_ flowcontrol.DispatchReservationTracker = &detector{}
)

// detector implements a saturation detector and scheduling filter based on active request concurrency.
type detector struct {
	config                       config
	typedName                    fwkplugin.TypedName
	logger                       logr.Logger
	inFlightLoadDataKey          fwkplugin.DataKey
	uncachedRequestTokensDataKey fwkplugin.DataKey
	dispatchReservations         sync.Map
	pendingDispatches            atomic.Int64
	// budget decides the limits this replica enforces. Nil when the configured
	// mode never divides them, so the configured limits apply unchanged.
	budget *replicaBudget
	// handle is read once, on the first evaluation, to see whether the in-flight
	// load this detector reads includes peers.
	handle fwkplugin.Handle
	// inFlightProducerName is the name the detector resolves on the handle to
	// read that producer's cross-replica configuration.
	inFlightProducerName string
	budgetOnce           sync.Once
}

// newDetector creates a new instance of the Concurrency Detector.
func newDetector(name string, cfg config, logger logr.Logger) *detector {
	typedName := fwkplugin.TypedName{
		Type: ConcurrencyDetectorType,
		Name: name,
	}

	pluginLogger := logger.WithName(typedName.String())
	pluginLogger.V(logutil.DEFAULT).Info("Creating new ConcurrencyDetector",
		"mode", cfg.mode,
		"maxConcurrency", cfg.maxConcurrency,
		"maxTokenConcurrency", cfg.maxTokenConcurrency,
		"headroom", cfg.headroom,
		"failOpen", !cfg.failClosed,
		"dynamicBudget", cfg.budgetMode)

	if cfg.headroom > 1.0 {
		pluginLogger.Info("Unusually high headroom configured; verify value is a fraction, not a percentage",
			"headroom", cfg.headroom,
			"effectiveBurst", fmt.Sprintf("%.0f%%", cfg.headroom*100))
	}

	producerName := cfg.inFlightLoadProducerName
	if producerName == "" {
		// Unset selects the unnamed instance, which the loader names after its
		// type, the same name the data key defaults to.
		producerName = inflightloadconstants.InFlightLoadProducerType
	}
	d := &detector{
		config:                       cfg,
		typedName:                    typedName,
		logger:                       pluginLogger,
		inFlightLoadDataKey:          attrconcurrency.InFlightLoadDataKey.WithNonEmptyProducerName(cfg.inFlightLoadProducerName),
		uncachedRequestTokensDataKey: attrconcurrency.UncachedRequestTokensDataKey.WithNonEmptyProducerName(cfg.inFlightLoadProducerName),
		inFlightProducerName:         producerName,
	}
	if cfg.dividesBudget() {
		d.budget = newReplicaBudget(cfg, name, pluginLogger)
	}
	return d
}

// setHandle supplies the plugin handle the budget resolves the syncer and the
// in-flight producer from.
func (d *detector) setHandle(handle fwkplugin.Handle) {
	d.handle = handle
}

// budgetLabel is the value of the detector label on the budget gauges.
func (d *detector) budgetLabel() string {
	return d.typedName.Name
}

// limits returns the request and token limits this replica enforces.
func (d *detector) limits() (requests, tokens int64) {
	if d.budget == nil {
		return d.config.maxConcurrency, d.config.maxTokenConcurrency
	}
	d.budgetOnce.Do(d.resolveBudget)
	return d.budget.limits()
}

// resolveBudget settles, once, whether this detector's mode is safe for the
// in-flight load it reads, and how that load relates to peers.
//
// A producer that leaves syncCrossReplicaState at its default publishes its
// load to the syncer, which adds every replica's load to the value this
// detector reads. That value then already covers the deployment, so its limit
// must not be divided as well. Two consequences:
//
//   - budgetAlways is only correct against load that excludes peers. With a
//     synced producer and a configured syncer it would halve real capacity, so
//     it falls back to the full limit and logs once. Neither the syncer nor an
//     auto-created producer is on the handle when the factory runs, so this is
//     the earliest the check can happen.
//   - budgetOnPeerLoss divides only when peer values are stale, which only
//     means something if the load includes them. Against a local producer it
//     has no peer values to lose, so it divides unconditionally like
//     budgetAlways.
func (d *detector) resolveBudget() {
	syncsPeers, known := d.inFlightLoadSyncsPeers()
	if !known {
		// The producer is not on the handle yet. Assume the common case, a
		// synced producer, so the limit stays divided and peer freshness is
		// required; the load is local until the producer exists anyway.
		return
	}
	d.applyBudgetResolution(syncsPeers, d.handle != nil && d.handle.CrossReplicaSyncer() != nil)
}

// applyBudgetResolution settles the peer-load scope, and whether the mode is
// safe, from the in-flight producer's cross-replica behaviour and whether a
// syncer is configured.
func (d *detector) applyBudgetResolution(syncsPeers, haveSyncer bool) {
	logger := d.logger
	if !syncsPeers {
		logger.V(logutil.DEFAULT).Info("Flow-control budget divides the full limit: the in-flight load excludes peers",
			"inFlightLoadProducer", d.inFlightProducerName, "dynamicBudget", d.config.budgetMode)
		d.budget.setPeerLoadScope(peerLoadLocal)
		return
	}
	if d.config.budgetMode != budgetAlways {
		d.budget.setPeerLoadScope(peerLoadSynced)
		return
	}
	if !haveSyncer {
		// No syncer: the producer's synced load has no peers to add, so it is
		// local and the limit can be divided.
		d.budget.setPeerLoadScope(peerLoadLocal)
		return
	}
	logger.Error(nil, "dynamicBudget \"always\" is invalid with a cross-replica syncer and an in-flight producer that syncs "+
		"cross-replica state: the synced load already covers the whole deployment, so dividing the limit would halve capacity. "+
		"Falling back to the full limit; use dynamicBudget \"onPeerLoss\" instead.",
		"inFlightLoadProducer", d.inFlightProducerName)
	d.budget.fallBackToFullLimit()
}

// inFlightLoadSyncsPeers reports whether the in-flight producer this detector
// reads publishes its load to the cross-replica syncer. known is false when the
// producer is not on the handle.
func (d *detector) inFlightLoadSyncsPeers() (syncsPeers, known bool) {
	if d.handle == nil {
		return false, false
	}
	plugin := d.handle.Plugin(d.inFlightProducerName)
	if plugin == nil {
		return false, false
	}
	contributor, ok := plugin.(datalayer.CrossReplicaContributor)
	if !ok {
		// A producer that does not contribute to cross-replica state keeps its
		// load local.
		return false, true
	}
	return !contributor.CrossReplicaState().SyncDisabled, true
}

// TypedName returns the type and name tuple of this plugin instance.
func (d *detector) TypedName() fwkplugin.TypedName {
	return d.typedName
}

func (d *detector) Consumes() fwkplugin.DataDependencies {
	required := map[fwkplugin.DataKey]any{
		d.inFlightLoadDataKey: attrconcurrency.InFlightLoad{},
	}
	if d.config.mode == modeTokens || d.config.mode == modeHybrid {
		required[d.uncachedRequestTokensDataKey] = attrconcurrency.UncachedRequestTokens{}
	}
	return fwkplugin.DataDependencies{
		Required: required,
	}
}

func (d *detector) getLoad(m datalayer.AttributeMap) *attrconcurrency.InFlightLoad {
	if val, ok := m.Get(d.inFlightLoadDataKey); ok {
		if load, ok := val.(*attrconcurrency.InFlightLoad); ok {
			return load
		}
	}

	return &attrconcurrency.InFlightLoad{}
}

// ReserveDispatch accounts for a request in the interval after flow-control dispatch and before
// the in-flight load producer publishes it through PreRequest.
func (d *detector) ReserveDispatch(requestID string) bool {
	if requestID == "" {
		return false
	}
	if _, loaded := d.dispatchReservations.LoadOrStore(requestID, struct{}{}); loaded {
		return false
	}
	d.pendingDispatches.Add(1)
	return true
}

// ReleaseDispatch removes a dispatch reservation. Duplicate and unknown releases are no-ops.
func (d *detector) ReleaseDispatch(requestID string) bool {
	if requestID == "" {
		return false
	}
	if _, loaded := d.dispatchReservations.LoadAndDelete(requestID); !loaded {
		return false
	}
	d.pendingDispatches.Add(-1)
	return true
}

// getIncomingTokens returns the uncached tokens the current request would add to the endpoint.
func (d *detector) getIncomingTokens(m datalayer.AttributeMap) int64 {
	if val, ok := m.Get(d.uncachedRequestTokensDataKey); ok {
		if tokens, ok := val.(*attrconcurrency.UncachedRequestTokens); ok && tokens.Tokens > 0 {
			return tokens.Tokens
		}
	}
	return 0
}

// Saturation calculates the saturation level of the pool.
//
// In "requests" and "tokens" mode it returns an aggregate signal, evaluated as:
//
//	Saturation = Total Inflight / Total Capacity.
//
// In "hybrid" mode saturation is instead evaluated per endpoint as
// max(requestRatio, tokenRatio) and averaged across endpoints. Evaluating each
// endpoint independently ensures an endpoint saturated on either dimension is
// reflected in the pool signal.
//
// The inflight counts are read from the InFlightLoad attribute, which the
// InFlightLoadProducer increments at dispatch and decrements on request
// completion, so the signal already reflects completions with no scrape lag.
func (d *detector) Saturation(_ context.Context, endpoints []datalayer.Endpoint) float64 {
	if len(endpoints) == 0 {
		return 1.0
	}

	maxRequests, maxTokens := d.limits()
	var reqInflight, reqCapacity, tokInflight, tokCapacity int64
	var endpointCount int
	var hybridSatSum float64
	for _, e := range endpoints {
		if e == nil {
			continue
		}

		endpointCount++
		reqCapacity += maxRequests
		tokCapacity += maxTokens

		if e.GetMetadata() == nil {
			continue
		}

		load := d.getLoad(e.GetAttributes())
		reqInflight += load.Requests
		tokInflight += load.Tokens
		hybridSatSum += max(
			ratio(load.Requests, maxRequests),
			ratio(load.Tokens, maxTokens),
		)
	}
	pendingDispatches := d.pendingDispatches.Load()

	switch d.config.mode {
	case modeTokens:
		return ratio(tokInflight, tokCapacity)
	case modeHybrid:
		if endpointCount == 0 {
			return 1.0
		}
		return max(
			hybridSatSum/float64(endpointCount),
			ratio(reqInflight+pendingDispatches, reqCapacity),
		)
	default:
		return ratio(reqInflight+pendingDispatches, reqCapacity)
	}
}

// ratio computes inflight/capacity, failing closed (1.0) when capacity is zero.
func ratio(inflight, capacity int64) float64 {
	if capacity == 0 {
		return 1.0
	}
	return float64(inflight) / float64(capacity)
}

// Filter blocks traffic to specific endpoints that would exceed their safety limits.
//
// It applies a relaxed limit (Capacity * (1 + Headroom)) to allow for scheduling flexibility and burst tolerance.
// In "tokens" and "hybrid" mode the endpoint's token load includes the uncached tokens this request would add
// to it, so an endpoint is dropped when admitting the request would take it over the limit. An endpoint with no
// in-flight tokens always passes the token check: it is the best placement the pool can offer a request that
// is larger than the limit. In "hybrid" mode an endpoint is also dropped when its request load reaches the limit.
// If all endpoints are filtered out, the filter fails open and returns all endpoints, unless FailOpen is false.
func (d *detector) Filter(
	_ context.Context,
	_ *fwksched.InferenceRequest,
	endpoints []fwksched.Endpoint,
) []fwksched.Endpoint {
	// Pre-allocate assuming most endpoints will pass the filter to minimize allocations.
	filtered := make([]fwksched.Endpoint, 0, len(endpoints))

	reqMax, tokMax := d.limits()
	reqLimit := int64(float64(reqMax) * (1.0 + d.config.headroom))
	tokLimit := int64(float64(tokMax) * (1.0 + d.config.headroom))
	countIncoming := d.config.mode == modeTokens || d.config.mode == modeHybrid

	for _, e := range endpoints {
		if e == nil {
			continue
		}
		load := d.getLoad(e)
		var incomingTokens int64
		if countIncoming {
			incomingTokens = d.getIncomingTokens(e)
		}

		if d.admits(load, incomingTokens, reqLimit, tokLimit) {
			filtered = append(filtered, e)
		}
	}
	if len(filtered) == 0 && !d.config.failClosed {
		for _, e := range endpoints {
			if e != nil {
				filtered = append(filtered, e)
			}
		}
	}
	return filtered
}

// FailsOpen reports whether Filter returns every endpoint when all are over
// their limit, which hides from a caller that no endpoint has room.
func (d *detector) FailsOpen() bool {
	return !d.config.failClosed
}

// admits reports whether an endpoint can take the request within its safety limit for the active mode.
// An endpoint must be below each limit, and its token load plus the request's tokens must fit within
// the token limit unless the endpoint has no in-flight tokens.
func (d *detector) admits(load *attrconcurrency.InFlightLoad, incomingTokens, reqLimit, tokLimit int64) bool {
	tokensFit := load.Tokens < tokLimit && (load.Tokens == 0 || load.Tokens+incomingTokens <= tokLimit)
	switch d.config.mode {
	case modeTokens:
		return tokensFit
	case modeHybrid:
		return load.Requests < reqLimit && tokensFit
	default:
		return load.Requests < reqLimit
	}
}
