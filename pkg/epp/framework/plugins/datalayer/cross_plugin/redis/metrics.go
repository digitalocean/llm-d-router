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
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
	compbasemetrics "k8s.io/component-base/metrics"

	metricsutil "github.com/llm-d/llm-d-router/pkg/common/observability/metrics"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	eppmetrics "github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

const (
	directionPublish = "publish"
	directionRead    = "read"

	// resultSuccess and resultError are the values of the result label on
	// valkeyCommands: whether the command completed, with an error the server
	// returned or not.
	resultSuccess = "success"
	resultError   = "error"
)

// valkeyLatencyBuckets is a sub-millisecond-first ladder for Redis round trips.
// GeneralLatencyBuckets starts at 5ms, which hides the range that separates a
// healthy Valkey from a slow one.
var valkeyLatencyBuckets = []float64{
	0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2,
}

var (
	streamBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_stream_bytes_total",
			Help:      metricsutil.HelpMsgWithStability("Encoded element bytes this replica appended to its streams (publish) or read from peer streams (read).", compbasemetrics.ALPHA),
		},
		[]string{"state_key", "direction"},
	)

	streamBudgetExhausted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_stream_budget_exhausted_total",
			Help:      metricsutil.HelpMsgWithStability("Endpoint publishes or peer reads deferred to a later tick because maxBytesPerSecond was spent.", compbasemetrics.ALPHA),
		},
		[]string{"state_key", "direction"},
	)

	streamResets = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_stream_resets_total",
			Help:      metricsutil.HelpMsgWithStability("Streams this replica deleted and started republishing under a new epoch.", compbasemetrics.ALPHA),
		},
		[]string{"state_key"},
	)

	streamPeerLag = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_stream_peer_lag_generations",
			Help:      metricsutil.HelpMsgWithStability("Elements a peer has published that this replica has not read yet, observed after each peer read.", compbasemetrics.ALPHA),
			Buckets:   append([]float64{0}, prometheus.ExponentialBuckets(1, 4, 10)...),
		},
		[]string{"state_key"},
	)

	setBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_set_batch_duration_seconds",
			Help:      metricsutil.HelpMsgWithStability("Duration of one SetBatch call, which publishes every endpoint for one state key. Compare with syncInterval.", compbasemetrics.ALPHA),
			Buckets:   metricsutil.GeneralLatencyBuckets,
		},
		[]string{"state_key"},
	)

	valkeyCommands = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_valkey_commands_total",
			Help:      metricsutil.HelpMsgWithStability("Valkey commands this replica sent, by command and outcome. Each command in a pipeline is counted once.", compbasemetrics.ALPHA),
		},
		[]string{"op", "result"},
	)

	valkeyCommandDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_valkey_command_duration_seconds",
			Help:      metricsutil.HelpMsgWithStability("Wall time of Valkey commands this replica sent. Commands sent in one pipeline share the pipeline's duration.", compbasemetrics.ALPHA),
			Buckets:   valkeyLatencyBuckets,
		},
		[]string{"op"},
	)

	valkeyUp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_valkey_up",
			Help:      metricsutil.HelpMsgWithStability("1 when the last Valkey command reached the server, 0 when it failed to. 0 until this replica has reached the server once.", compbasemetrics.ALPHA),
		},
	)

	activePeers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_active_peers",
			Help:      metricsutil.HelpMsgWithStability("Peer replicas whose values this replica holds in a cached peer aggregate for a state key. A peer drops out when its values expire or its endpoint is deleted.", compbasemetrics.ALPHA),
		},
		[]string{"state_key"},
	)

	peerIndexEntries = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_peer_index_entries",
			Help:      metricsutil.HelpMsgWithStability("Sequenced elements this replica merged from peer streams into its cached peer aggregates for a state key. A whole-value peer counts as one entry.", compbasemetrics.ALPHA),
		},
		[]string{"state_key"},
	)

	lastSuccessfulSync = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_last_successful_sync_timestamp_seconds",
			Help:      metricsutil.HelpMsgWithStability("Unix time of the last publication that failed for no endpoint of a state key. time() minus this value is the sync lag.", compbasemetrics.ALPHA),
		},
		[]string{"state_key"},
	)

	streamBudgetBytesPerSecond = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_stream_budget_bytes_per_second",
			Help:      metricsutil.HelpMsgWithStability("Effective per-replica stream byte budget, from maxBytesPerSecond. The publish and read budgets are separate.", compbasemetrics.ALPHA),
		},
		[]string{"direction"},
	)

	warmStartTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_warm_start_total",
			Help:      metricsutil.HelpMsgWithStability("Warm starts this replica attempted, by outcome. completed read every endpoint, timeout and aborted left part of the peer state unread.", compbasemetrics.ALPHA),
		},
		[]string{"outcome"},
	)

	warmStartDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_warm_start_duration_seconds",
			Help:      metricsutil.HelpMsgWithStability("Time one warm start took. This is readiness latency added at startup when Valkey is slow or unreachable.", compbasemetrics.ALPHA),
			Buckets:   metricsutil.GeneralLatencyBuckets,
		},
		[]string{"outcome"},
	)

	warmStartPeers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "redis_syncer_warm_start_peers",
			Help:      metricsutil.HelpMsgWithStability("Peer values the last warm start merged. Zero after a failed warm start means this replica started with local state only.", compbasemetrics.ALPHA),
		},
	)

	// registeredMetrics lists every collector this plugin registers.
	registeredMetrics = []prometheus.Collector{
		streamBytes, streamBudgetExhausted, streamResets, streamPeerLag, setBatchDuration,
		valkeyCommands, valkeyCommandDuration, valkeyUp, activePeers, peerIndexEntries,
		lastSuccessfulSync, streamBudgetBytesPerSecond,
		warmStartTotal, warmStartDuration, warmStartPeers,
	}
)

// registerMetrics registers the syncer metrics. The vectors are package-level,
// so a second syncer instance registering them again is a no-op.
func registerMetrics(registerer prometheus.Registerer) error {
	for _, collector := range registeredMetrics {
		if err := registerer.Register(collector); err != nil {
			var alreadyRegistered prometheus.AlreadyRegisteredError
			if errors.As(err, &alreadyRegistered) && alreadyRegistered.ExistingCollector == collector {
				continue
			}
			return fmt.Errorf("redis-syncer: register metric: %w", err)
		}
	}
	return nil
}

func observeSetBatch(key fwkdl.StateKey, start time.Time) {
	setBatchDuration.WithLabelValues(string(key)).Observe(time.Since(start).Seconds())
}

func observeSuccessfulSync(key fwkdl.StateKey, at time.Time) {
	lastSuccessfulSync.WithLabelValues(string(key)).Set(float64(at.UnixNano()) / 1e9)
}

// observeStreamBudget publishes the configured per-replica stream budget.
func observeStreamBudget(bytesPerSecond int64) {
	for _, direction := range []string{directionPublish, directionRead} {
		streamBudgetBytesPerSecond.WithLabelValues(direction).Set(float64(bytesPerSecond))
	}
}

// warmStartOutcome values report how a warm start ended.
const (
	warmStartCompleted = "completed"
	warmStartTimeout   = "timeout"
	warmStartAborted   = "aborted"
)

// observeWarmStart records one warm start attempt.
func observeWarmStart(outcome string, result fwkdl.WarmStartResult, elapsed time.Duration) {
	warmStartTotal.WithLabelValues(outcome).Inc()
	warmStartDuration.WithLabelValues(outcome).Observe(elapsed.Seconds())
	warmStartPeers.Set(float64(result.Peers))
}

// peerGauges maintains activePeers and peerIndexEntries from cache writes
// instead of by scanning the cache at scrape time. A peer counts once per state
// key however many endpoints it appears under, so activePeers is the size of
// the live peer set this replica sees. A window retained from a departed
// replica keeps contributing peerIndexEntries, because its prompts are still
// merged, but it is not an active peer.
type peerGauges struct {
	mu sync.Mutex
	// active maps a state key to the live peer replica IDs its cached
	// aggregates hold, and how many cached endpoints hold each.
	active map[string]map[string]int
	// entries is how many peer elements the state key's cached aggregates hold.
	entries map[string]int
}

func newPeerGauges() *peerGauges {
	return &peerGauges{active: map[string]map[string]int{}, entries: map[string]int{}}
}

// replace moves the bookkeeping of one cached hash key from prev to next. A nil
// next removes the entry; a nil prev adds one.
func (g *peerGauges) replace(key fwkdl.StateKey, prev, next *aggregateCacheEntry) {
	label := string(key)
	g.mu.Lock()
	defer g.mu.Unlock()
	counted := g.active[label]
	if counted == nil {
		counted = map[string]int{}
		g.active[label] = counted
	}
	if prev != nil {
		for _, id := range prev.activePeerIDs {
			if counted[id]--; counted[id] <= 0 {
				delete(counted, id)
			}
		}
		g.entries[label] -= prev.peerEntries
	}
	if next != nil {
		for _, id := range next.activePeerIDs {
			counted[id]++
		}
		g.entries[label] += next.peerEntries
	}

	activePeers.WithLabelValues(label).Set(float64(len(counted)))
	peerIndexEntries.WithLabelValues(label).Set(float64(g.entries[label]))
}

// valkeyMetricsHook records what this replica sends to Valkey.
type valkeyMetricsHook struct{}

func (valkeyMetricsHook) DialHook(next goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (valkeyMetricsHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmd)
		observeValkeyCommand(cmd.Name(), start, err)
		return err
	}
}

func (valkeyMetricsHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			// Each command keeps its own error once the pipeline has run, so a
			// pipeline that failed as a whole still reports per command.
			observeValkeyCommand(cmd.Name(), start, cmd.Err())
		}
		return err
	}
}

// observeValkeyCommand counts one command and updates the reachability gauge.
// A nil reply and an error the server returned both prove the server answered;
// only a transport failure (dial, read, write, or deadline) means it did not.
// A command the caller cancelled says nothing about reachability, so it leaves
// the gauge alone.
func observeValkeyCommand(op string, start time.Time, err error) {
	op = strings.ToUpper(op)
	valkeyCommandDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())

	var redisErr goredis.Error
	switch {
	case err == nil, errors.Is(err, goredis.Nil):
		valkeyCommands.WithLabelValues(op, resultSuccess).Inc()
		valkeyUp.Set(1)
	case errors.As(err, &redisErr):
		valkeyCommands.WithLabelValues(op, resultError).Inc()
		valkeyUp.Set(1)
	default:
		valkeyCommands.WithLabelValues(op, resultError).Inc()
		if !errors.Is(err, context.Canceled) {
			valkeyUp.Set(0)
		}
	}
}
