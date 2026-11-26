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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/cross_plugin/internal/crossreplica"
)

// newFactorySyncer builds a factory-configured syncer pointed at a miniredis,
// with extra plugin parameters given as a JSON object body without braces.
func newFactorySyncer(t *testing.T, extra string) *RedisSyncer {
	t.Helper()
	server := miniredis.RunT(t)
	plugin, err := RedisSyncerFactory("redis", json.NewDecoder(strings.NewReader(
		factoryParams(server.Addr(), extra),
	)), nil)
	require.NoError(t, err)
	syncer := plugin.(*RedisSyncer)
	t.Cleanup(func() { require.NoError(t, syncer.client.Close()) })
	return syncer
}

// factoryParams builds plugin parameters from a JSON fragment of extra fields.
func factoryParams(address, extra string) string {
	extra = strings.TrimSpace(extra)
	extra = strings.TrimPrefix(extra, "{")
	extra = strings.TrimSuffix(extra, "}")
	extra = strings.TrimSuffix(strings.TrimSpace(extra), ",")
	if extra == "" {
		return fmt.Sprintf(`{"address": %q}`, address)
	}
	return fmt.Sprintf(`{"address": %q, %s}`, address, extra)
}

func TestFactorySplitsTTLsByKind(t *testing.T) {
	for _, tc := range []struct {
		name         string
		params       string
		wantState    time.Duration
		wantInflight time.Duration
		wantPrefix   time.Duration
	}{
		{
			name:         "no TTL parameters take the per-kind defaults",
			params:       `{}`,
			wantState:    crossreplica.DefaultStateTTL,
			wantInflight: crossreplica.DefaultStateTTL,
			wantPrefix:   10 * time.Minute,
		},
		{
			name:         "stateTTL alone keeps every kind on it",
			params:       `{"stateTTL": "5s"}`,
			wantState:    5 * time.Second,
			wantInflight: 5 * time.Second,
			wantPrefix:   5 * time.Second,
		},
		{
			name:         "a per-kind TTL overrides stateTTL",
			params:       `{"stateTTL": "5s", "prefixStateTTL": "15m"}`,
			wantState:    5 * time.Second,
			wantInflight: 5 * time.Second,
			wantPrefix:   15 * time.Minute,
		},
		{
			name:         "per-kind TTLs alone leave stateTTL at its default",
			params:       `{"inflightStateTTL": "1s", "prefixStateTTL": "30m"}`,
			wantState:    crossreplica.DefaultStateTTL,
			wantInflight: time.Second,
			wantPrefix:   30 * time.Minute,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			syncer := newFactorySyncer(t, tc.params)

			require.Equal(t, tc.wantState, syncer.stateTTL)
			require.Equal(t, tc.wantInflight, syncer.inflightStateTTL)
			require.Equal(t, tc.wantPrefix, syncer.prefixStateTTL)
			require.Equal(t, tc.wantInflight, syncer.inflightTTL())
			require.Equal(t, tc.wantPrefix, syncer.prefixTTL())
		})
	}
}

func TestFactoryRejectsInvalidPerKindTTLs(t *testing.T) {
	for _, tc := range []struct{ name, params, wantErr string }{
		{name: "unparseable inflight TTL", params: `{"inflightStateTTL": "soon"}`, wantErr: "invalid inflightStateTTL"},
		{name: "unparseable prefix TTL", params: `{"prefixStateTTL": "soon"}`, wantErr: "invalid prefixStateTTL"},
		{name: "zero inflight TTL", params: `{"inflightStateTTL": "0s"}`, wantErr: "inflightStateTTL must be at least 1ms"},
		{name: "sub-millisecond prefix TTL", params: `{"prefixStateTTL": "500us"}`, wantErr: "prefixStateTTL must be at least 1ms"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			plugin, err := RedisSyncerFactory("redis", json.NewDecoder(strings.NewReader(
				factoryParams(server.Addr(), tc.params),
			)), nil)

			require.ErrorContains(t, err, tc.wantErr)
			require.Nil(t, plugin)
		})
	}
}

func TestInflightValuesExpireOnTheShortTTL(t *testing.T) {
	server := miniredis.RunT(t)
	syncer, _ := newTestSyncer(t, server, testReplicaID)
	syncer.inflightStateTTL = 2 * time.Second
	syncer.prefixStateTTL = 10 * time.Minute

	require.NoError(t, syncer.set(context.Background(), testStateKey, testEndpointID, 4, sumInts))

	hashKey := syncer.hashKey(testStateKey, testEndpointID)
	require.Equal(t, syncer.inflightStateTTL, server.TTL(hashKey))
}

func TestPrefixWindowsExpireOnTheLongTTL(t *testing.T) {
	server := miniredis.RunT(t)
	a := newStreamReplica(t, server, "epp-a", true)
	a.syncer.inflightStateTTL = 2 * time.Second
	a.syncer.prefixStateTTL = 10 * time.Minute
	a.append([]uint64{1, 2})

	a.publish(t)

	require.Equal(t, a.syncer.prefixStateTTL, server.TTL(a.hashKey()))
	require.Equal(t, a.syncer.prefixStateTTL, server.TTL(a.streamKey()))
}

func TestWholeValuePrefixWindowUsesTheLongTTL(t *testing.T) {
	server := miniredis.RunT(t)
	syncer, _ := newTestSyncer(t, server, testReplicaID)
	syncer.inflightStateTTL = time.Second
	syncer.prefixStateTTL = 10 * time.Minute
	window := &attrprefix.PrefixHashSet{Prompts: [][]uint64{{1, 2}}}
	spec := fwkdl.CrossReplicaSpec{
		StateKey:  streamStateKey,
		Read:      func(string) fwkdl.Cloneable { return window },
		Aggregate: func(values []any) any { return values[0] },
	}

	// incremental is off, so the window is published as a whole value.
	require.NoError(t, syncer.Set(context.Background(), spec, testEndpointID))

	require.Equal(t, syncer.prefixStateTTL, server.TTL(syncer.hashKey(streamStateKey, testEndpointID)))
}

func TestCrashedReplicaLeavesPrefixWindowAfterInflightExpiry(t *testing.T) {
	server := miniredis.RunT(t)
	syncer, _ := newTestSyncer(t, server, testReplicaID)
	syncer.inflightStateTTL = time.Second
	syncer.prefixStateTTL = 10 * time.Minute
	ctx := context.Background()

	// A departed replica's last publications: one accounting value and one window.
	load, err := crossreplica.EncodeValue(7)
	require.NoError(t, err)
	server.HSet(syncer.hashKey(testStateKey, testEndpointID), "epp-b", string(load))
	server.SetTTL(syncer.hashKey(testStateKey, testEndpointID), syncer.inflightStateTTL)
	window := &attrprefix.PrefixHashSet{Prompts: [][]uint64{{1, 2}}}
	encodedWindow, err := crossreplica.EncodeValue(window)
	require.NoError(t, err)
	windowKey := syncer.hashKey(streamStateKey, testEndpointID)
	server.HSet(windowKey, "epp-b", string(encodedWindow))
	server.SetTTL(windowKey, syncer.prefixStateTTL)

	server.FastForward(syncer.inflightStateTTL + time.Second)

	loadExists, err := syncer.client.HExists(ctx, syncer.hashKey(testStateKey, testEndpointID), "epp-b").Result()
	require.NoError(t, err)
	require.False(t, loadExists, "a crashed replica's in-flight load drops out quickly")
	windowExists, err := syncer.client.HExists(ctx, windowKey, "epp-b").Result()
	require.NoError(t, err)
	require.True(t, windowExists, "its prefix window is kept for the prefix retention TTL")
}
