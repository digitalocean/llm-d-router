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

package concurrency

import (
	"errors"
	"fmt"

	"k8s.io/utils/ptr"
)

// apiConfig represents the external configuration schema for the concurrency detector.
// It dictates how the plugin calculates pool-level saturation (to trigger backpressure) and
// endpoint-level limits (to filter out overloaded candidates during routing).
//
// It is designed to be deserialized from JSON via the plugin's raw parameters.
type apiConfig struct {
	// MaxConcurrency defines the request-based saturation threshold for an endpoint.
	//
	// This limit serves as the "ideal" request capacity for a single endpoint. The plugin aggregates
	// the active requests across all endpoints and compares them against the aggregate pool capacity
	// (Total Endpoints * MaxConcurrency) to compute PoolSaturation. When the pool approaches or
	// exceeds this aggregate capacity, the Flow Controller triggers backpressure to buffer new
	// traffic until capacity becomes available.
	//
	// Defaults to 100 if unset.
	MaxConcurrency *int64 `json:"maxConcurrency,omitempty"`

	// Headroom defines the allowed burst capacity above the ideal threshold (MaxConcurrency or
	// MaxTokenConcurrency), expressed as a multiplier (e.g., 0.2 for 20%).
	//
	// This parameter decouples pool-level backpressure from individual endpoint routing. While
	// PoolSaturation uses the strict ideal capacity to manage overall pool load, the Filter logic
	// uses EndpointLimit (Capacity * (1 + Headroom)) to determine if a specific endpoint can accept
	// more work.
	//
	// Example: MaxConcurrency=100, Headroom=0.2.
	//
	//   - Backpressure: An endpoint with 110 requests is considered 110% full, pushing pool averages up.
	//   - Routing: The endpoint's hard filter limit is 120. The scheduling layer can still send it
	//     requests (e.g., to satisfy high affinity) until it hits 120.
	//
	// Defaults to 0.0 (no burst allowed) if unset.
	Headroom *float64 `json:"headroom,omitempty"`

	// ConcurrencyMode defines the mode of concurrency detection.
	//
	// Valid values are:
	// - "requests": use discrete request counts for capacity accounting.
	// - "tokens": use estimated token counts for capacity accounting.
	// - "hybrid": evaluate each endpoint as the more constraining of its request and
	//   token ratios; pool saturation is the average of these across endpoints.
	//
	// Defaults to "requests" if unset.
	ConcurrencyMode *concurrencyMode `json:"concurrencyMode,omitempty"`

	// MaxTokenConcurrency defines the token-based saturation threshold for an endpoint.
	//
	// This is the "tokens" mode equivalent of MaxConcurrency. It represents the "ideal" token
	// capacity per endpoint. It drives both the pool saturation calculation (for backpressure) and,
	// combined with Headroom, the per-endpoint filtering limits (for routing).
	//
	// Defaults to 1000000 if unset.
	MaxTokenConcurrency      *int64 `json:"maxTokenConcurrency,omitempty"`
	InFlightLoadProducerName string `json:"inFlightLoadProducerName,omitempty"`

	// DynamicBudget divides MaxConcurrency and MaxTokenConcurrency across the
	// active EPP replicas. Each replica admits up to
	// ceil(global / live_replicas * BudgetOversubscription) of ITS OWN in-flight
	// load, so the shares add up to the deployment-wide limit.
	//
	// This bounds cluster-wide admission even during a burst, when replicas have
	// not yet seen each other's admissions through the cross-replica syncer. The
	// cost is early shedding when traffic is unevenly spread between replicas: a
	// replica admits at most its share even while another sits idle.
	//
	// Defaults to false, which enforces the full limit on every replica
	// measured against the load the detector reads, including peer load when the
	// in-flight producer syncs across replicas.
	DynamicBudget *bool `json:"dynamicBudget,omitempty"`

	// BudgetOversubscription scales the per-replica share computed when
	// DynamicBudget is set, to tolerate uneven load across replicas. 1.0 keeps
	// the shares adding up to exactly the deployment-wide limit.
	//
	// Defaults to 1.0 if unset.
	BudgetOversubscription *float64 `json:"budgetOversubscription,omitempty"`

	// ExpectedReplicas is the replica count the limit is divided by before the
	// cross-replica syncer reports one, such as during a cold start. With a
	// syncer that reports no count and no expected count, the limit is not
	// divided. Zero or unset leaves it unknown.
	ExpectedReplicas *int `json:"expectedReplicas,omitempty"`

	// FailOpen controls the scheduling filter when every candidate endpoint is over its limit.
	//
	// When true, the filter returns all candidates and the scorers pick the least loaded one.
	// When false, the filter returns no candidates, so the profile finds no endpoint and the
	// request fails instead of overloading an endpoint.
	//
	// Defaults to true if unset.
	FailOpen *bool `json:"failOpen,omitempty"`
}

// concurrencyMode is the concurrency detection mode.
type concurrencyMode string

const (
	// modeRequests uses request count for concurrency detection.
	modeRequests concurrencyMode = "requests"
	// modeTokens uses token count for concurrency detection.
	modeTokens concurrencyMode = "tokens"
	// modeHybrid uses endpoint average saturation across both request and token counts.
	modeHybrid concurrencyMode = "hybrid"
)

const (
	// defaultMaxConcurrency is the safe baseline for many LLM serving engines.
	defaultMaxConcurrency int64 = 100
	// defaultHeadroom is the default burst allowance (0%).
	defaultHeadroom float64 = 0.0
	// defaultConcurrencyMode is used when ConcurrencyMode is unset.
	defaultConcurrencyMode = modeRequests
	// defaultMaxTokenConcurrency is the default maximum number of tokens allowed per endpoint.
	defaultMaxTokenConcurrency int64 = 1000000
	// defaultFailOpen is used when FailOpen is unset.
	defaultFailOpen = true
	// defaultDynamicBudget keeps the full deployment-wide limit on every replica.
	defaultDynamicBudget = false
	// defaultBudgetOversubscription keeps the per-replica shares adding up to
	// exactly the deployment-wide limit.
	defaultBudgetOversubscription = 1.0
)

// config is the internal, fully-validated configuration used by the detector.
type config struct {
	maxConcurrency           int64
	headroom                 float64
	mode                     concurrencyMode
	maxTokenConcurrency      int64
	inFlightLoadProducerName string
	// failClosed makes the filter return no endpoints when every candidate is over its limit.
	failClosed bool
	// dynamicBudget divides the limits across the live replicas.
	dynamicBudget bool
	// budgetOversubscription scales each replica's share of the limits.
	budgetOversubscription float64
	// expectedReplicas divides the limits before the replica count is known.
	expectedReplicas int
}

// buildConfig applies the configuration lifecycle (defaulting and validation) and translates the
// external schema into the internal domain model.
// The provided apiConfig is copied to prevent mutation side-effects.
func buildConfig(apiCfg *apiConfig) (*config, error) {
	var safeCfg apiConfig
	if apiCfg != nil {
		safeCfg = *apiCfg
	}

	applyDefaults(&safeCfg)

	if err := validateConfig(&safeCfg); err != nil {
		return nil, fmt.Errorf("invalid concurrency detector configuration: %w", err)
	}

	return &config{
		maxConcurrency:           *safeCfg.MaxConcurrency,
		headroom:                 *safeCfg.Headroom,
		mode:                     *safeCfg.ConcurrencyMode,
		maxTokenConcurrency:      *safeCfg.MaxTokenConcurrency,
		inFlightLoadProducerName: safeCfg.InFlightLoadProducerName,
		failClosed:               !*safeCfg.FailOpen,
		dynamicBudget:            *safeCfg.DynamicBudget,
		budgetOversubscription:   *safeCfg.BudgetOversubscription,
		expectedReplicas:         coldStartReplicas(&safeCfg),
	}, nil
}

// coldStartReplicas returns the configured cold-start replica count, or zero
// when unset.
func coldStartReplicas(cfg *apiConfig) int {
	if cfg.ExpectedReplicas == nil {
		return 0
	}
	return *cfg.ExpectedReplicas
}

// applyDefaults populates unset fields in the external configuration with their standard defaults.
func applyDefaults(cfg *apiConfig) {
	if cfg.MaxConcurrency == nil {
		cfg.MaxConcurrency = ptr.To(defaultMaxConcurrency)
	}
	if cfg.Headroom == nil {
		cfg.Headroom = ptr.To(defaultHeadroom)
	}
	if cfg.ConcurrencyMode == nil {
		cfg.ConcurrencyMode = ptr.To(defaultConcurrencyMode)
	}
	if cfg.MaxTokenConcurrency == nil {
		cfg.MaxTokenConcurrency = ptr.To(defaultMaxTokenConcurrency)
	}
	if cfg.FailOpen == nil {
		cfg.FailOpen = ptr.To(defaultFailOpen)
	}
	if cfg.DynamicBudget == nil {
		cfg.DynamicBudget = ptr.To(defaultDynamicBudget)
	}
	if cfg.BudgetOversubscription == nil {
		cfg.BudgetOversubscription = ptr.To(defaultBudgetOversubscription)
	}
}

// validateConfig checks the constraints of the fully defaulted configuration.
// It aggregates all validation failures.
func validateConfig(cfg *apiConfig) error {
	var errs []error

	if cfg.MaxConcurrency != nil && *cfg.MaxConcurrency <= 0 {
		errs = append(errs, fmt.Errorf("maxConcurrency must be strictly positive, got %d", *cfg.MaxConcurrency))
	}
	if cfg.Headroom != nil && *cfg.Headroom < 0.0 {
		errs = append(errs, fmt.Errorf("headroom must be a non-negative value, got %f", *cfg.Headroom))
	}
	if cfg.MaxTokenConcurrency != nil && *cfg.MaxTokenConcurrency <= 0 {
		errs = append(errs, fmt.Errorf("maxTokenConcurrency must be strictly positive, got %d", *cfg.MaxTokenConcurrency))
	}

	if cfg.ConcurrencyMode != nil {
		switch *cfg.ConcurrencyMode {
		case modeRequests, modeTokens, modeHybrid:
			// Valid
		default:
			errs = append(errs, fmt.Errorf("unsupported concurrencyMode: %q", *cfg.ConcurrencyMode))
		}
	}

	if cfg.BudgetOversubscription != nil && !(*cfg.BudgetOversubscription > 0.0) {
		errs = append(errs, fmt.Errorf("budgetOversubscription must be strictly positive, got %f", *cfg.BudgetOversubscription))
	}
	if cfg.ExpectedReplicas != nil && *cfg.ExpectedReplicas < 0 {
		errs = append(errs, fmt.Errorf("expectedReplicas must be non-negative, got %d", *cfg.ExpectedReplicas))
	}

	return errors.Join(errs...)
}
