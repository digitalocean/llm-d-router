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
	if cfg.dynamicBudget && handle != nil {
		// The cross-replica syncer is added to the handle after plugins are
		// instantiated, so the replica count is resolved on first use.
		detector.budget.setSource(&handleReplicaCounter{handle: handle})
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
	// budget decides the limits this replica enforces. Nil when dynamicBudget
	// is off, so the configured limits apply unchanged.
	budget *replicaBudget
	// handle is read on the first evaluation to resolve the in-flight load
	// producer whose own counters a divided budget is compared against.
	handle fwkplugin.Handle
	// inFlightProducerName is the name the detector resolves on the handle to
	// reach that producer.
	inFlightProducerName string
	budgetOnce           sync.Once
	// localLoad returns this replica's own in-flight load and whether the
	// producer reports it. It is resolved from the handle on first evaluation
	// and is nil-safe through localLoadFor.
	localLoad atomic.Pointer[datalayer.LocalInFlightLoadReader]
	// localLoadMissing is set once resolving showed no producer that reports
	// local load, so the check is not repeated on every evaluation.
	localLoadMissing atomic.Bool
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
		"dynamicBudget", cfg.dynamicBudget)

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
	if cfg.dynamicBudget {
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
	return d.budget.limits()
}

// resolveLocalLoad resolves, once, the in-flight load producer that reports
// this replica's own load. With dynamicBudget on, the detector compares the
// divided budget against that local load: the installed InFlightLoad attribute
// is replaced with a cross-replica aggregate when sync is on, and counting peer
// load against a per-replica share would block well below the share.
//
// The producer is looked up on the first evaluation because plugins are
// instantiated before they are all on the handle. A producer that does not
// report local load leaves the detector on the aggregate, which is what it
// reads with dynamicBudget off, and says so once.
func (d *detector) resolveLocalLoad() {
	d.budgetOnce.Do(func() {
		if d.handle == nil {
			return
		}
		reader, ok := d.handle.Plugin(d.inFlightProducerName).(datalayer.LocalInFlightLoadReader)
		if !ok {
			d.localLoadMissing.Store(true)
			d.logger.Info("The in-flight load producer does not report local load; the divided flow-control budget is measured "+
				"against the load the detector reads, which may include peers",
				"inFlightLoadProducer", d.inFlightProducerName)
			return
		}
		d.localLoad.Store(&reader)
	})
}

// localLoadFor returns this replica's own in-flight load for an endpoint. It
// reports false when the producer does not expose local load, in which case the
// caller falls back to the endpoint's attribute map.
func (d *detector) localLoadFor(endpointID string) (requests, tokens int64, ok bool) {
	d.resolveLocalLoad()
	if d.localLoadMissing.Load() {
		return 0, 0, false
	}
	reader := d.localLoad.Load()
	if reader == nil {
		return 0, 0, false
	}
	requests, tokens = (*reader).LocalInFlightLoad(endpointID)
	return requests, tokens, true
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

// loadFor returns the in-flight load the detector counts for an endpoint.
//
// With dynamicBudget off this is the InFlightLoad attribute, which the
// cross-replica publisher replaces with an aggregate across peers, so the full
// deployment-wide limit is compared against a deployment-wide load. With it on,
// this replica's own counters are used instead, because the limit is only this
// replica's share of the deployment.
func (d *detector) loadFor(endpointID string, attributes datalayer.AttributeMap) *attrconcurrency.InFlightLoad {
	if d.budget != nil && endpointID != "" {
		if requests, tokens, ok := d.localLoadFor(endpointID); ok {
			return &attrconcurrency.InFlightLoad{Requests: requests, Tokens: tokens}
		}
	}
	return d.getLoad(attributes)
}

// endpointLoad is loadFor for an endpoint, keyed by the endpoint's identity.
// Both the datalayer and scheduling Endpoint interfaces expose metadata and the
// attribute map, so one helper serves Saturation and Filter.
func (d *detector) endpointLoad(meta *datalayer.EndpointMetadata, attributes datalayer.AttributeMap) *attrconcurrency.InFlightLoad {
	id := ""
	if meta != nil {
		id = meta.GetNamespacedName().String()
	}
	return d.loadFor(id, attributes)
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
// With dynamicBudget on, the counts are this replica's own instead, so the
// divided capacity is compared against the load that fills it.
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

		load := d.endpointLoad(e.GetMetadata(), e.GetAttributes())
		reqInflight += load.Requests
		tokInflight += load.Tokens
		hybridSatSum += max(
			ratio(load.Requests, maxRequests),
			ratio(load.Tokens, maxTokens),
		)
	}
	pendingDispatches := d.pendingDispatches.Load()
	if d.budget != nil {
		observeLocalLoad(d.budgetLabel(), reqInflight, tokInflight)
	}

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
		load := d.endpointLoad(e.GetMetadata(), e)
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
