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

// ReplicaCounter is an optional interface for a CrossReplicaSyncer that can
// count the EPP replicas currently sharing state. A replica counts as live
// while the store still holds a value it published within its TTL, so the
// count follows the same heartbeat the syncer already relies on: this replica
// plus every peer whose fields have not expired.
//
// Consumers use the count to divide a deployment-wide limit across the
// replicas that will serve requests, such as a dynamic flow-control budget.
type ReplicaCounter interface {
	// ActiveReplicas returns the number of live replicas, this one included,
	// and whether any count has been observed yet. It is a cached read that
	// does not access the store. After the store becomes unreachable the last
	// observed count is returned, so a caller keeps the share it computed
	// instead of reverting to the full deployment-wide limit. ok is false
	// until the replica has completed its first successful publication.
	ActiveReplicas() (count int, ok bool)
}

// PeerLoadReporter is an optional interface for a CrossReplicaSyncer that can
// say whether the peer values it merges are recent.
//
// A consumer that reads a value the syncer already aggregates across replicas
// should not also divide that value's limit: the aggregate covers the whole
// deployment, so dividing it would halve capacity that no replica can use. It
// should divide only while the aggregate is missing or stale, which is what
// PeerLoadFresh reports.
type PeerLoadReporter interface {
	// PeerLoadFresh reports whether the syncer read or merged peer values
	// within its peer-state TTL. It is a cached read that does not access the
	// store: it reports false once the TTL passes without a successful
	// publication, so an unreachable store turns it false without any further
	// work.
	PeerLoadFresh() bool
}
