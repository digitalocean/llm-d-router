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
// while the store still holds a liveness value it refreshed within its TTL, so
// the count follows the same heartbeat the syncer already relies on: this
// replica plus every peer whose liveness has not expired.
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

// Drainer is an optional interface for a plugin that changes behavior once the
// replica starts draining on SIGTERM. The replica keeps serving its in-flight
// requests but takes no new ones, so a syncer stops reporting it as live.
type Drainer interface {
	StartDrain()
}

// LocalInFlightLoadReader is an optional interface for a plugin that reports
// this replica's own in-flight load for one endpoint, excluding anything other
// replicas published.
//
// A local view is what a limit divided across replicas must be compared
// against. A CrossReplicaContributor's installed attribute may be replaced with
// an aggregate across replicas, which is the right input for routing and for a
// deployment-wide limit, but against a per-replica share it would count peer
// load as this replica's own and block well below the share.
type LocalInFlightLoadReader interface {
	// LocalInFlightLoad returns this replica's own in-flight requests and
	// tokens for endpointID, which is EndpointMetadata.ID.String().
	LocalInFlightLoad(endpointID string) (requests, tokens int64)
}
