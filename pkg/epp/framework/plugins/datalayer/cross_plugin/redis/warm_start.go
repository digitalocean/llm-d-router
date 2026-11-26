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
	"slices"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

var _ fwkdl.CrossReplicaWarmStarter = (*RedisSyncer)(nil)

// warmStartBatch bounds how many endpoints one warm-start read covers before
// its aggregates are stored. It matches the publication batch, so the pipeline
// depth and the memory one read holds stay on the same order as a sync tick.
const warmStartBatch = batchSize

// WarmStart reads and merges the peer aggregates for every published endpoint,
// so a starting replica has peer prefix state before it reports ready instead
// of waiting for traffic to arrive.
//
// The read is bounded by warmStartTimeout. When it expires, or when Valkey is
// unreachable, WarmStart returns the aggregates it did merge and an error
// describing the shortfall. The caller reports ready anyway: peer prefix state
// improves routing, but the replica serves correctly with local state alone.
func (s *RedisSyncer) WarmStart(ctx context.Context, specs []fwkdl.CrossReplicaSpec, endpointIDs []string) fwkdl.WarmStartResult {
	if len(specs) == 0 || len(endpointIDs) == 0 {
		return fwkdl.WarmStartResult{}
	}
	timeout := s.warmStartTimeout
	if timeout <= 0 {
		timeout = defaultWarmStartTimeout
	}
	start := time.Now()
	warmCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result := fwkdl.WarmStartResult{}
	var errs []error
	for _, spec := range specs {
		peers, endpoints, err := s.warmStartSpec(warmCtx, spec, endpointIDs)
		result.Peers += peers
		result.Endpoints += endpoints
		if err != nil {
			errs = append(errs, fmt.Errorf("key %s: %w", spec.StateKey, err))
		}
	}
	if ctx.Err() != nil {
		errs = append(errs, fmt.Errorf("warm start aborted: %w", ctx.Err()))
	} else if warmCtx.Err() != nil {
		errs = append(errs, fmt.Errorf("warm start timed out after %s: %w", timeout, warmCtx.Err()))
	}
	result.Err = errors.Join(errs...)

	outcome := warmStartCompleted
	switch {
	case ctx.Err() != nil:
		outcome = warmStartAborted
	case warmCtx.Err() != nil:
		outcome = warmStartTimeout
	}
	observeWarmStart(outcome, result, time.Since(start))

	logger := ctrl.LoggerFrom(ctx)
	if result.Err != nil {
		logger.Info("redis-syncer: warm start incomplete, serving with the peer state that was read",
			"endpoints", result.Endpoints, "peers", result.Peers, "error", result.Err.Error())
		return result
	}
	logger.V(logutil.DEBUG).Info("redis-syncer: warm start merged peer state",
		"endpoints", result.Endpoints, "peers", result.Peers)
	return result
}

// warmStartSpec reads the peer aggregate of every endpoint for one spec and
// returns the peers merged and the endpoints read. It stops at the first read
// error and keeps what it already merged, so a Valkey outage costs one failed
// round trip rather than one per endpoint.
func (s *RedisSyncer) warmStartSpec(ctx context.Context, spec fwkdl.CrossReplicaSpec, endpointIDs []string) (peers, endpoints int, err error) {
	for start := 0; start < len(endpointIDs); start += warmStartBatch {
		if ctx.Err() != nil {
			return peers, endpoints, ctx.Err()
		}
		chunk := endpointIDs[start:min(start+warmStartBatch, len(endpointIDs))]
		merged, err := s.warmStartChunk(ctx, spec, chunk)
		peers += merged
		endpoints += len(chunk)
		if err != nil {
			return peers, endpoints, err
		}
	}
	return peers, endpoints, nil
}

// warmStartChunk reads one chunk's hashes, reads the stream elements the peer
// markers name, and caches each aggregate. It returns how many peer values the
// chunk merged.
func (s *RedisSyncer) warmStartChunk(ctx context.Context, spec fwkdl.CrossReplicaSpec, chunk []string) (int, error) {
	pipe := s.client.Pipeline()
	pubs := make([]*publication, 0, len(chunk))
	for _, endpointID := range chunk {
		value := spec.Read(endpointID)
		if value == nil {
			continue
		}
		pub := &publication{
			endpointID: endpointID,
			hashKey:    s.hashKey(spec.StateKey, endpointID),
			value:      value,
		}
		pub.result = pipe.HGetAll(ctx, pub.hashKey)
		pubs = append(pubs, pub)
	}
	if len(pubs) == 0 {
		return 0, nil
	}
	// Per-command errors are handled below; the chunk fails only when no
	// command completed, which is the case a transport failure produces.
	if _, err := pipe.Exec(ctx); err != nil && !slices.ContainsFunc(pubs, func(pub *publication) bool {
		return pub.result.Err() == nil
	}) {
		return 0, fmt.Errorf("read peer hashes: %w", err)
	}

	peers := 0
	reads := make([]*publication, 0, len(pubs))
	for _, pub := range pubs {
		if pub.result.Err() != nil {
			continue
		}
		if s.readsStreams(pub) {
			reads = append(reads, pub)
			continue
		}
		peers += s.cachePeerAggregate(ctx, spec.StateKey, pub.hashKey, pub.value, pub.result.Val(), spec.Aggregate)
	}
	if len(reads) > 0 {
		before := s.cachedPeers(spec.StateKey, chunk)
		s.readStreams(ctx, spec.StateKey, spec.Aggregate, reads)
		peers += s.cachedPeers(spec.StateKey, chunk) - before
	}
	return peers, nil
}

// cachedPeers counts the peers the cached aggregates of chunk hold for key.
func (s *RedisSyncer) cachedPeers(key fwkdl.StateKey, chunk []string) int {
	peers := 0
	for _, endpointID := range chunk {
		v, ok := s.cache.Load(s.hashKey(key, endpointID))
		if !ok {
			continue
		}
		entry := v.(*aggregateCacheEntry)
		peers += len(entry.streams) + entry.legacyPeers
	}
	return peers
}
