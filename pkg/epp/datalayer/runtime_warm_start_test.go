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

package datalayer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
)

// warmStartSyncer is a fake CrossReplicaSyncer that records the endpoints a
// warm start was asked to read and blocks until it is released.
type warmStartSyncer struct {
	mu     sync.Mutex
	specs  []fwkdl.CrossReplicaSpec
	ids    []string
	calls  atomic.Int64
	result fwkdl.WarmStartResult
	// release, when non-nil, blocks WarmStart until it is closed.
	release chan struct{}
}

var (
	_ fwkdl.CrossReplicaSyncer      = (*warmStartSyncer)(nil)
	_ fwkdl.CrossReplicaWarmStarter = (*warmStartSyncer)(nil)
)

func (s *warmStartSyncer) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: "fake-syncer", Name: "fake"}
}

func (s *warmStartSyncer) Set(context.Context, fwkdl.CrossReplicaSpec, string) error { return nil }

func (s *warmStartSyncer) Get(context.Context, fwkdl.CrossReplicaSpec, string) (any, bool, error) {
	return nil, false, nil
}

func (s *warmStartSyncer) Delete(context.Context, fwkdl.StateKey, string) error { return nil }

func (s *warmStartSyncer) GetOrSet(_ context.Context, _ fwkdl.StateKey, _ string, candidate any) (any, bool, error) {
	return candidate, false, nil
}

func (s *warmStartSyncer) WarmStart(_ context.Context, specs []fwkdl.CrossReplicaSpec, ids []string) fwkdl.WarmStartResult {
	s.calls.Add(1)
	s.mu.Lock()
	s.specs, s.ids = specs, ids
	s.mu.Unlock()
	if s.release != nil {
		<-s.release
	}
	return s.result
}

// warmStartContributor declares a cross-replica spec so the publisher has one.
type warmStartContributor struct {
	spec fwkdl.CrossReplicaSpec
}

func (c *warmStartContributor) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: "fake-contributor", Name: "fake"}
}

func (c *warmStartContributor) CrossReplicaState() fwkdl.CrossReplicaSpec { return c.spec }

// newWarmStartRuntime returns a Runtime wired to a syncer with one contributor
// spec, plus a registered endpoint.
func newWarmStartRuntime(t *testing.T, syncer fwkdl.CrossReplicaSyncer) *Runtime {
	t.Helper()
	contributor := &warmStartContributor{spec: fwkdl.CrossReplicaSpec{
		StateKey:  fwkdl.StateKey("approx-prefix:fake"),
		Read:      func(string) fwkdl.Cloneable { return nil },
		Aggregate: func(values []any) any { return values[0] },
	}}
	runtime := newTestRuntimeForWarmStart(t, syncer, contributor)
	return runtime
}

// newTestRuntimeForWarmStart builds a Runtime whose publisher holds contributor.
func newTestRuntimeForWarmStart(t *testing.T, syncer fwkdl.CrossReplicaSyncer, contributor fwkdl.CrossReplicaContributor) *Runtime {
	t.Helper()
	runtime := NewRuntime(time.Second)
	runtime.crossReplicaPub = &crossReplicaPublisher{
		syncer:       syncer,
		contributors: []fwkdl.CrossReplicaContributor{contributor},
		interval:     time.Millisecond,
	}
	runtime.crossReplicaPub.registerEndpoint(types.NamespacedName{Namespace: "ns", Name: "pod"})
	return runtime
}

func TestCheckReadyWaitsForWarmStart(t *testing.T) {
	syncer := &warmStartSyncer{release: make(chan struct{})}
	runtime := newWarmStartRuntime(t, syncer)

	// Before the notification sources have synced, readiness reports that.
	if err := runtime.CheckReady(); err == nil {
		t.Fatal("CheckReady() before Start returned nil")
	}

	runtime.notificationSyncMu.Lock()
	runtime.notificationSyncs = []*notificationInitialSync{}
	runtime.warmStartCtx = context.Background()
	runtime.notificationSyncMu.Unlock()

	// The first check after the sources sync starts the warm start and reports
	// that it is in progress.
	err := runtime.CheckReady()
	if err == nil || !strings.Contains(err.Error(), "warm start") {
		t.Fatalf("CheckReady() while warm starting = %v, want a warm start error", err)
	}
	requireEventually(t, func() bool { return syncer.calls.Load() == 1 })

	close(syncer.release)
	requireEventually(t, func() bool { return runtime.CheckReady() == nil })
	if syncer.calls.Load() != 1 {
		t.Fatalf("WarmStart called %d times, want 1", syncer.calls.Load())
	}
}

func TestWarmStartReadsRegisteredEndpoints(t *testing.T) {
	syncer := &warmStartSyncer{}
	runtime := newWarmStartRuntime(t, syncer)
	runtime.warmStartCtx = context.Background()
	startWarmStartAndWait(t, runtime)

	syncer.mu.Lock()
	defer syncer.mu.Unlock()
	if len(syncer.specs) != 1 {
		t.Fatalf("WarmStart specs = %d, want 1", len(syncer.specs))
	}
	if len(syncer.ids) != 1 || syncer.ids[0] != "ns/pod" {
		t.Fatalf("WarmStart endpoint IDs = %v, want [ns/pod]", syncer.ids)
	}
}

func TestWarmStartFailureStillReportsReady(t *testing.T) {
	syncer := &warmStartSyncer{result: fwkdl.WarmStartResult{Err: errors.New("valkey unreachable")}}
	runtime := newWarmStartRuntime(t, syncer)
	runtime.warmStartCtx = context.Background()

	startWarmStartAndWait(t, runtime)

	if err := runtime.CheckReady(); err != nil {
		t.Fatalf("CheckReady() after a failed warm start = %v, want nil", err)
	}
}

func TestCheckReadySkipsWarmStartWithoutASyncer(t *testing.T) {
	runtime := NewRuntime(time.Second)
	runtime.notificationSyncMu.Lock()
	runtime.notificationSyncs = []*notificationInitialSync{}
	runtime.notificationSyncMu.Unlock()

	if err := runtime.CheckReady(); err != nil {
		t.Fatalf("CheckReady() without a cross-replica syncer = %v", err)
	}
}

// startWarmStartAndWait starts the warm start and waits for it to finish.
func startWarmStartAndWait(t *testing.T, runtime *Runtime) {
	t.Helper()
	runtime.notificationSyncMu.Lock()
	runtime.notificationSyncs = []*notificationInitialSync{}
	runtime.notificationSyncMu.Unlock()
	// The first CheckReady starts the warm start; the second one waits for it.
	if err := runtime.CheckReady(); err != nil && !strings.Contains(err.Error(), "warm start") {
		t.Fatalf("CheckReady() = %v", err)
	}
	requireEventually(t, func() bool { return runtime.CheckReady() == nil })
}

// requireEventually polls cond until it is true or the deadline passes.
func requireEventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before the deadline")
}
