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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	ctrl "sigs.k8s.io/controller-runtime"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/cross_plugin/internal/crossreplica"
)

const (
	RedisSyncerType = "redis-syncer"
	defaultAddress  = "localhost:6379"
	// defaultCoordinationTTL is how long request-level coordination values stay.
	defaultCoordinationTTL = 180 * time.Second
	// defaultInflightStateTTL is how long a replica's in-flight load and budget
	// accounting count after its last refresh when inflightStateTTL is unset and
	// stateTTL is not configured either. It matches DefaultStateTTL: a crashed
	// replica's load drops out within a few sync ticks.
	defaultInflightStateTTL = crossreplica.DefaultStateTTL
	// defaultPrefixStateTTL is how long a replica's prefix windows stay in Valkey
	// after its last refresh when prefixStateTTL is unset and stateTTL is not
	// configured either. Prefix windows are expensive to rebuild from traffic and
	// cheap to keep, so they outlive the per-replica accounting values.
	defaultPrefixStateTTL = 10 * time.Minute
	// defaultWarmStartTimeout bounds the peer-state read a starting replica does
	// before it reports ready. It is generous enough for a cold peer aggregate
	// over a slow link and short enough that an unreachable Valkey delays
	// readiness by half a minute, not forever.
	defaultWarmStartTimeout = 30 * time.Second
	batchSize               = 100
	batchConcurrency        = 8
)

var _ fwkdl.CrossReplicaSyncer = (*RedisSyncer)(nil)
var _ fwkdl.CrossReplicaBatchSyncer = (*RedisSyncer)(nil)
var _ fwkdl.ReplicaCounter = (*RedisSyncer)(nil)

type redisConfig struct {
	Address           string `json:"address"`
	Password          string `json:"password"`
	PasswordEnv       string `json:"passwordEnv"`
	PasswordFile      string `json:"passwordFile"`
	DB                int    `json:"db"`
	StateTTL          string `json:"stateTTL"`
	InflightStateTTL  string `json:"inflightStateTTL"`
	PrefixStateTTL    string `json:"prefixStateTTL"`
	CoordinationTTL   string `json:"coordinationTTL"`
	WarmStartTimeout  string `json:"warmStartTimeout"`
	WindowReplication string `json:"windowReplication"`
	MaxBytesPerSecond int64  `json:"maxBytesPerSecond"`
}

// RedisSyncer is a CrossReplicaSyncer backed by Redis for cross-replica
// state sharing. Set prepares each endpoint's peer aggregate, and Get combines
// it with the live local value. Only a successful Set replaces the aggregate, so
// while Redis is unreachable the last known peer state keeps counting.
type RedisSyncer struct {
	typedName fwkplugin.TypedName
	replicaID string
	client    *goredis.Client
	// draining stops liveness refreshes once shutdown starts. See StartDrain.
	draining atomic.Bool
	// stateTTL expires every kind of synced state. It is the only TTL unless
	// inflightStateTTL or prefixStateTTL override it, which keeps a
	// configuration that sets stateTTL alone behaving as before the split.
	stateTTL time.Duration
	// inflightStateTTL expires per-replica load and budget-accounting values, so
	// a crashed replica's in-flight load drops out within a couple of sync ticks.
	inflightStateTTL time.Duration
	// prefixStateTTL expires prefix windows. They are expensive to rebuild from
	// traffic, so they outlive the accounting values by design.
	prefixStateTTL  time.Duration
	coordinationTTL time.Duration
	// warmStartTimeout bounds WarmStart.
	warmStartTimeout time.Duration
	cache            sync.Map
	// peerGauges keeps activePeers and peerIndexEntries in step with cache.
	peerGauges *peerGauges
	// incremental publishes SequencedValues as streams instead of whole values.
	incremental bool
	// cursors maps a hash key to the streamCursor of this replica's stream.
	cursors sync.Map
	// prefixKeys records the state keys this replica published prefix windows
	// under, so Delete retains their Redis state on the prefix TTL instead of
	// removing it. Values are struct{}.
	prefixKeys sync.Map
	// epochs numbers the stream epochs this replica starts.
	epochs        atomic.Uint64
	publishBudget *byteBudget
	readBudget    *byteBudget
	// replicaCounts is the live replica count across the endpoint hashes this
	// replica reads. See observeReplicas.
	replicas replicaCounts
}

// inflightTTL is the TTL for whole values such as in-flight load and budget
// accounting. A zero inflightStateTTL falls back to stateTTL.
func (s *RedisSyncer) inflightTTL() time.Duration {
	if s.inflightStateTTL > 0 {
		return s.inflightStateTTL
	}
	return s.stateTTL
}

// prefixTTL is the TTL for prefix windows and their streams. A zero
// prefixStateTTL falls back to stateTTL.
func (s *RedisSyncer) prefixTTL() time.Duration {
	if s.prefixStateTTL > 0 {
		return s.prefixStateTTL
	}
	return s.stateTTL
}

// heartbeatInterval is how often a stream publisher rewrites its marker even
// when the window has not changed. The marker's timestamp is what tells peers
// the replica is still live while its window is retained past the in-flight TTL,
// so the interval is a fraction of that TTL: a peer must not look dead between
// two heartbeats.
func (s *RedisSyncer) heartbeatInterval() time.Duration {
	heartbeat := s.inflightTTL() / 3
	if heartbeat < crossreplica.MinimumDuration {
		return crossreplica.MinimumDuration
	}
	return heartbeat
}

// valueTTL is the TTL for a published value: prefix windows outlive accounting
// values. Sequenced values are the prefix windows; every other synced value is
// per-replica accounting.
func (s *RedisSyncer) valueTTL(value any) time.Duration {
	if s.retainsPrefix(value) {
		return s.prefixTTL()
	}
	return s.inflightTTL()
}

// retainsPrefix reports whether value is prefix-window state, which is kept on
// the prefix TTL and outlives the replica that published it.
func (s *RedisSyncer) retainsPrefix(value any) bool {
	_, ok := value.(fwkdl.SequencedValue)
	return ok
}

type aggregateCacheEntry struct {
	value any
	// streams holds the windows rebuilt from peers that publish streams.
	streams map[string]*streamPeer
	// legacyPeers is how many whole-value peers the cached aggregate includes.
	legacyPeers int
	// peerIDs names every peer the cached aggregate includes, for
	// peerIndexEntries and the retained-window bookkeeping.
	peerIDs []string
	// activePeerIDs names the peers that refreshed within the in-flight TTL, for
	// activePeers. A retained window from a departed replica stays in peerIDs.
	activePeerIDs []string
	// peerEntries counts the sequenced elements merged from peers, for
	// peerIndexEntries. A whole-value peer counts as one.
	peerEntries int
}

func parseTTL(name, raw string, defaultValue time.Duration) (time.Duration, error) {
	if raw == "" {
		return defaultValue, nil
	}
	ttl, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("redis-syncer: invalid %s %q: %w", name, raw, err)
	}
	if ttl < crossreplica.MinimumDuration {
		return 0, fmt.Errorf("redis-syncer: %s must be at least %s, got %s", name, crossreplica.MinimumDuration, ttl)
	}
	return ttl, nil
}

// resolveKindTTL parses one per-kind TTL. An unset per-kind parameter falls back
// to stateTTL when the configuration sets it, so a configuration that sets only
// stateTTL expires every kind of state on that TTL exactly as before the kinds
// were split. Otherwise it takes the kind's own default.
func resolveKindTTL(name, raw string, stateTTL time.Duration, stateTTLSet bool, defaultValue time.Duration) (time.Duration, error) {
	fallback := defaultValue
	if stateTTLSet {
		fallback = stateTTL
	}
	return parseTTL(name, raw, fallback)
}

func resolvePassword(cfg redisConfig) (string, error) {
	if cfg.Password != "" {
		return "", errors.New("redis-syncer: password must use passwordEnv or passwordFile")
	}
	if cfg.PasswordEnv != "" && cfg.PasswordFile != "" {
		return "", errors.New("redis-syncer: passwordEnv and passwordFile are mutually exclusive")
	}
	if cfg.PasswordEnv != "" {
		password, ok := os.LookupEnv(cfg.PasswordEnv)
		if !ok {
			return "", fmt.Errorf("redis-syncer: password environment variable %q is not set", cfg.PasswordEnv)
		}
		return password, nil
	}
	if cfg.PasswordFile != "" {
		password, err := os.ReadFile(cfg.PasswordFile)
		if err != nil {
			return "", fmt.Errorf("redis-syncer: read password file %q: %w", cfg.PasswordFile, err)
		}
		return strings.TrimRight(string(password), "\r\n"), nil
	}
	return "", nil
}

func RedisSyncerFactory(name string, params *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	var cfg redisConfig
	if params != nil {
		params.DisallowUnknownFields()
		if err := params.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("redis-syncer: invalid parameters: %w", err)
		}
	}
	if cfg.Address == "" {
		cfg.Address = defaultAddress
	}

	stateTTL, err := parseTTL("stateTTL", cfg.StateTTL, crossreplica.DefaultStateTTL)
	if err != nil {
		return nil, err
	}
	inflightStateTTL, err := resolveKindTTL("inflightStateTTL", cfg.InflightStateTTL, stateTTL, cfg.StateTTL != "", defaultInflightStateTTL)
	if err != nil {
		return nil, err
	}
	prefixStateTTL, err := resolveKindTTL("prefixStateTTL", cfg.PrefixStateTTL, stateTTL, cfg.StateTTL != "", defaultPrefixStateTTL)
	if err != nil {
		return nil, err
	}
	coordinationTTL, err := parseTTL("coordinationTTL", cfg.CoordinationTTL, defaultCoordinationTTL)
	if err != nil {
		return nil, err
	}
	warmStartTimeout, err := parseTTL("warmStartTimeout", cfg.WarmStartTimeout, defaultWarmStartTimeout)
	if err != nil {
		return nil, err
	}
	password, err := resolvePassword(cfg)
	if err != nil {
		return nil, err
	}
	var incremental bool
	switch cfg.WindowReplication {
	case "", windowReplicationFull:
	case windowReplicationIncremental:
		incremental = true
	default:
		return nil, fmt.Errorf("redis-syncer: windowReplication %q must be one of %s|%s", cfg.WindowReplication, windowReplicationFull, windowReplicationIncremental)
	}
	if cfg.MaxBytesPerSecond < 0 {
		return nil, fmt.Errorf("redis-syncer: maxBytesPerSecond must be positive, got %d", cfg.MaxBytesPerSecond)
	}
	if cfg.MaxBytesPerSecond == 0 {
		cfg.MaxBytesPerSecond = defaultMaxBytesPerSecond
	}
	if handle != nil && handle.Metrics() != nil {
		if err := registerMetrics(handle.Metrics()); err != nil {
			return nil, err
		}
	}
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}
	replicaID := hostname + "-" + uuid.NewString()

	client := goredis.NewClient(&goredis.Options{
		Addr:                  cfg.Address,
		Password:              password,
		DB:                    cfg.DB,
		ContextTimeoutEnabled: true,
	})
	client.AddHook(valkeyMetricsHook{})
	if handle != nil && handle.Context() != nil {
		context.AfterFunc(handle.Context(), func() { _ = client.Close() })
	}
	observeStreamBudget(cfg.MaxBytesPerSecond)
	// Report unreachable until a command proves otherwise, so a replica that has
	// never reached Valkey does not look healthy by omitting the series.
	valkeyUp.Set(0)

	return &RedisSyncer{
		typedName:        fwkplugin.TypedName{Type: RedisSyncerType, Name: name},
		replicaID:        replicaID,
		client:           client,
		stateTTL:         stateTTL,
		inflightStateTTL: inflightStateTTL,
		prefixStateTTL:   prefixStateTTL,
		coordinationTTL:  coordinationTTL,
		warmStartTimeout: warmStartTimeout,
		peerGauges:       newPeerGauges(),
		incremental:      incremental,
		publishBudget:    newByteBudget(cfg.MaxBytesPerSecond),
		readBudget:       newByteBudget(cfg.MaxBytesPerSecond),
	}, nil
}

func (s *RedisSyncer) TypedName() fwkplugin.TypedName {
	return s.typedName
}

func (s *RedisSyncer) hashKey(key fwkdl.StateKey, endpointID string) string {
	return string(key) + ":" + endpointID
}

// coordKey namespaces request-level values away from endpoint hashes.
func (s *RedisSyncer) coordKey(key fwkdl.StateKey, id string) string {
	return "coord:" + string(key) + ":" + id
}

// set publishes this replica's value and prepares the peer aggregate returned
// by get.
func (s *RedisSyncer) set(ctx context.Context, key fwkdl.StateKey, endpointID string, value any, aggregate func([]any) any) error {
	logger := ctrl.LoggerFrom(ctx)

	data, err := crossreplica.EncodeValue(value)
	if err != nil {
		return fmt.Errorf("redis-syncer: encode: %w", err)
	}

	hashKey := s.hashKey(key, endpointID)
	ttl := s.valueTTL(value)
	pipe := s.client.TxPipeline()
	pipe.HSet(ctx, hashKey, s.replicaID, data)
	pipe.HPExpire(ctx, hashKey, ttl, s.replicaID)
	pipe.PExpire(ctx, hashKey, ttl)
	if s.retainsPrefix(value) {
		s.prefixKeys.Store(key, struct{}{})
		// The liveness field outlives neither stateTTL nor inflightStateTTL; in
		// this path it only has to survive until the next tick rewrites it.
		s.livenessCommands(ctx, pipe, hashKey)
	} else {
		s.drainCommands(ctx, pipe, hashKey)
	}
	result := pipe.HGetAll(ctx, hashKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis-syncer: set and get aggregate; Redis 7.4 or newer with HPEXPIRE is required: %w", err)
	}

	raw, err := result.Result()
	if err != nil {
		return fmt.Errorf("redis-syncer: hgetall: %w", err)
	}
	peerCount := s.cachePeerAggregate(ctx, key, hashKey, value, raw, aggregate)
	observeSuccessfulSync(key, time.Now())

	if v := logger.V(logutil.DEBUG); v.Enabled() {
		v.Info("redis-syncer: Set", "key", string(key), "endpoint", endpointID, "replica", s.replicaID, "numReplicas", peerCount+1)
	}
	return nil
}

// cachePeerAggregate replaces the cached peer aggregate for hashKey and returns
// how many peers it holds. A hash with no peer value drops the cache entry.
//
// A whole-value peer cannot report its liveness inside its value, so the hash
// carries a liveness field per replica for this path. A peer whose field has
// expired keeps its value in the aggregate only while the value's own TTL keeps
// it readable, and it does not count as an active peer.
func (s *RedisSyncer) cachePeerAggregate(ctx context.Context, key fwkdl.StateKey, hashKey string, value any, raw map[string]string, aggregate func([]any) any) int {
	logger := ctrl.LoggerFrom(ctx)
	live := newLiveSet(raw)
	remoteValues := make([]any, 0, len(raw))
	peerIDs := make([]string, 0, len(raw))
	activePeerIDs := make([]string, 0, len(raw))
	for _, field := range slices.Sorted(maps.Keys(raw)) {
		if field == s.replicaID || strings.HasPrefix(field, refreshPrefix) || strings.HasPrefix(field, markerPrefix) || strings.HasPrefix(field, drainingPrefix) {
			continue
		}
		encoded := raw[field]
		remoteValue, err := crossreplica.DecodeValue([]byte(encoded), value)
		if err != nil {
			if v := logger.V(logutil.DEBUG); v.Enabled() {
				v.Info("redis-syncer: decode error", "field", field, "error", err)
			}
			continue
		}
		remoteValues = append(remoteValues, remoteValue)
		peerIDs = append(peerIDs, field)
		if live.isLive(field) {
			activePeerIDs = append(activePeerIDs, field)
		}
	}

	var entry *aggregateCacheEntry
	if len(remoteValues) > 0 {
		entry = &aggregateCacheEntry{
			value:         aggregate(remoteValues),
			peerIDs:       peerIDs,
			activePeerIDs: activePeerIDs,
			peerEntries:   len(remoteValues),
		}
	}
	s.storeAggregate(key, hashKey, entry)

	s.observeReplicas(hashKey, raw)
	return len(remoteValues)
}

// storeAggregate replaces the cached aggregate for hashKey and updates the peer
// gauges. A nil entry removes the cache entry.
func (s *RedisSyncer) storeAggregate(key fwkdl.StateKey, hashKey string, entry *aggregateCacheEntry) {
	var prev *aggregateCacheEntry
	if v, ok := s.cache.Load(hashKey); ok {
		prev = v.(*aggregateCacheEntry)
	}
	if entry == nil {
		s.cache.Delete(hashKey)
	} else {
		s.cache.Store(hashKey, entry)
	}
	if s.peerGauges != nil {
		s.peerGauges.replace(key, prev, entry)
	}
}

func (s *RedisSyncer) Set(ctx context.Context, spec fwkdl.CrossReplicaSpec, endpointID string) error {
	value := spec.Read(endpointID)
	if _, ok := value.(fwkdl.SequencedValue); ok {
		read := func(string) any { return value }
		return s.publishChunk(ctx, spec.StateKey, spec.Aggregate, []string{endpointID}, read)[endpointID]
	}
	return s.set(ctx, spec.StateKey, endpointID, value, spec.Aggregate)
}

func (s *RedisSyncer) SetBatch(ctx context.Context, spec fwkdl.CrossReplicaSpec, endpointIDs []string, timeout time.Duration) map[string]error {
	defer observeSetBatch(spec.StateKey, time.Now())
	failures := make(map[string]error)
	slots := make(chan struct{}, batchConcurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup
	remaining := endpointIDs
dispatch:
	for start := 0; start < len(endpointIDs); start += batchSize {
		if ctx.Err() != nil {
			break
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			break dispatch
		}
		end := min(start+batchSize, len(endpointIDs))
		chunk := endpointIDs[start:end]
		remaining = endpointIDs[end:]
		wg.Go(func() {
			defer func() { <-slots }()
			chunkCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			errs := s.setBatchChunk(chunkCtx, spec, chunk)
			mu.Lock()
			maps.Copy(failures, errs)
			mu.Unlock()
		})
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		for _, endpointID := range remaining {
			failures[endpointID] = err
		}
	}
	return failures
}

func (s *RedisSyncer) setBatchChunk(ctx context.Context, spec fwkdl.CrossReplicaSpec, chunk []string) map[string]error {
	read := func(endpointID string) any { return spec.Read(endpointID) }
	return s.publishChunk(ctx, spec.StateKey, spec.Aggregate, chunk, read)
}

type publication struct {
	endpointID string
	hashKey    string
	value      any
	commands   []goredis.Cmder
	result     *goredis.MapStringStringCmd
	// stream is set when value is published as a stream.
	stream *streamPublication
}

func (s *RedisSyncer) publishChunk(ctx context.Context, key fwkdl.StateKey, aggregate func([]any) any, chunk []string, read func(string) any) map[string]error {
	failures := make(map[string]error)
	if err := ctx.Err(); err != nil {
		for _, endpointID := range chunk {
			failures[endpointID] = err
		}
		return failures
	}
	logger := ctrl.LoggerFrom(ctx)

	pipe := s.client.TxPipeline()
	publications := make([]*publication, 0, len(chunk))
	for _, endpointID := range chunk {
		value := read(endpointID)
		pub := &publication{endpointID: endpointID, hashKey: s.hashKey(key, endpointID), value: value}
		if sequenced, ok := value.(fwkdl.SequencedValue); ok && s.incremental {
			s.prefixKeys.Store(key, struct{}{})
			pub.stream, pub.commands = s.queueStream(ctx, pipe, key, pub.hashKey, sequenced)
		} else {
			data, err := crossreplica.EncodeValue(value)
			if err != nil {
				failures[endpointID] = fmt.Errorf("redis-syncer: encode: %w", err)
				continue
			}
			ttl := s.valueTTL(value)
			pub.commands = []goredis.Cmder{
				pipe.HSet(ctx, pub.hashKey, s.replicaID, data),
				pipe.HPExpire(ctx, pub.hashKey, ttl, s.replicaID),
				pipe.PExpire(ctx, pub.hashKey, ttl),
			}
			if s.retainsPrefix(value) {
				s.prefixKeys.Store(key, struct{}{})
				pub.commands = append(pub.commands, s.livenessCommands(ctx, pipe, pub.hashKey)...)
			} else {
				pub.commands = append(pub.commands, s.drainCommands(ctx, pipe, pub.hashKey)...)
			}
		}
		pub.result = pipe.HGetAll(ctx, pub.hashKey)
		pub.commands = append(pub.commands, pub.result)
		publications = append(publications, pub)
	}
	if len(publications) == 0 {
		return failures
	}
	commands, execErr := pipe.Exec(ctx)
	if execFailedWholeChunk(execErr, commands) {
		for _, pub := range publications {
			if pub.stream != nil {
				s.unknownStreamOutcome(pub)
			}
			failures[pub.endpointID] = fmt.Errorf("redis-syncer: set and get aggregate; Redis 7.4 or newer with HPEXPIRE is required: %w", execErr)
		}
		return failures
	}
	var reads []*publication
	for _, pub := range publications {
		var err error
		for _, cmd := range pub.commands {
			if cmd.Err() != nil {
				err = cmd.Err()
				break
			}
		}
		if pub.stream != nil {
			s.finishStream(ctx, key, pub, err)
		}
		if err != nil {
			failures[pub.endpointID] = fmt.Errorf("redis-syncer: set and get aggregate; Redis 7.4 or newer with HPEXPIRE is required: %w", err)
			continue
		}
		if pub.stream != nil {
			s.ensureMarker(ctx, pub)
		}
		if s.readsStreams(pub) {
			reads = append(reads, pub)
			continue
		}
		peerCount := s.cachePeerAggregate(ctx, key, pub.hashKey, pub.value, pub.result.Val(), aggregate)
		if v := logger.V(logutil.DEBUG); v.Enabled() {
			v.Info("redis-syncer: Set", "key", string(key), "endpoint", pub.endpointID, "replica", s.replicaID, "numReplicas", peerCount+1)
		}
	}
	if len(reads) > 0 {
		s.readStreams(ctx, key, aggregate, reads)
	}
	if len(failures) == 0 {
		observeSuccessfulSync(key, time.Now())
	}
	return failures
}

// execFailedWholeChunk reports whether a TxPipeline Exec error means every
// command in the transaction failed. go-redis returns the first command error
// from Exec: when that error is a Redis error attached to one command (for
// example WRONGTYPE on one key), the other commands still ran and only that
// endpoint failed. EXECABORT, transport errors, context errors, and Redis
// errors attached to no command mean the transaction did not run or its
// outcome is unknown, so the whole chunk fails and caches stay.
func execFailedWholeChunk(execErr error, cmds []goredis.Cmder) bool {
	if execErr == nil {
		return false
	}
	var redisErr goredis.Error
	if !errors.As(execErr, &redisErr) || goredis.HasErrorPrefix(execErr, "EXECABORT") {
		return true
	}
	return !slices.ContainsFunc(cmds, func(cmd goredis.Cmder) bool {
		return errors.Is(cmd.Err(), execErr)
	})
}

// get combines the live local value with the peer aggregate prepared by set.
func (s *RedisSyncer) get(_ context.Context, key fwkdl.StateKey, endpointID string, local any, aggregate func([]any) any) any {
	hashKey := s.hashKey(key, endpointID)
	cached, ok := s.cache.Load(hashKey)
	if !ok {
		return local
	}
	return aggregate([]any{local, cached.(*aggregateCacheEntry).value})
}

func (s *RedisSyncer) Get(ctx context.Context, spec fwkdl.CrossReplicaSpec, endpointID string) (any, bool, error) {
	return s.get(ctx, spec.StateKey, endpointID, spec.Read(endpointID), spec.Aggregate), true, nil
}

func (s *RedisSyncer) delete(ctx context.Context, key fwkdl.StateKey, endpointID string) error {
	hashKey := s.hashKey(key, endpointID)
	s.storeAggregate(key, hashKey, nil)
	if !s.retainsKeyPrefix(key) {
		return s.client.HDel(ctx, hashKey, s.replicaID).Err()
	}
	// A prefix window is retained on its TTL so a restarted replica, or every
	// replica after a correlated crash, can rebuild it from Valkey. The value or
	// marker and the stream stay; only this replica's liveness goes, so the
	// window stops counting as an active peer while it stays readable.
	s.cursors.Delete(hashKey)
	return s.client.HDel(ctx, hashKey, refreshField(s.replicaID)).Err()
}

// retainsKeyPrefix reports whether this replica published prefix windows under
// key, which is what makes its Redis state worth retaining on Delete.
func (s *RedisSyncer) retainsKeyPrefix(key fwkdl.StateKey) bool {
	_, ok := s.prefixKeys.Load(key)
	return ok
}

func (s *RedisSyncer) Delete(ctx context.Context, key fwkdl.StateKey, endpointID string) error {
	return s.delete(ctx, key, endpointID)
}

// GetOrSet atomically returns the existing request-level value or stores candidate.
// The operation requires Redis 7.0 or newer for SET NX GET support.
func (s *RedisSyncer) GetOrSet(ctx context.Context, key fwkdl.StateKey, id string, candidate any) (any, bool, error) {
	if candidate == nil {
		return nil, false, errors.New("redis-syncer: getorset candidate must not be nil")
	}

	data, err := crossreplica.EncodeValue(candidate)
	if err != nil {
		return nil, false, fmt.Errorf("redis-syncer: getorset encode: %w", err)
	}

	stored, err := s.client.SetArgs(ctx, s.coordKey(key, id), data, goredis.SetArgs{
		Mode: "NX",
		Get:  true,
		TTL:  s.coordinationTTL,
	}).Result()
	if errors.Is(err, goredis.Nil) {
		return candidate, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redis-syncer: getorset: %w", err)
	}

	actual, err := crossreplica.DecodeValue([]byte(stored), candidate)
	if err != nil {
		return nil, false, fmt.Errorf("redis-syncer: getorset decode: %w", err)
	}
	return actual, true, nil
}
