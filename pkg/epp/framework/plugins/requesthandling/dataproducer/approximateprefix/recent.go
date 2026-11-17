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

package approximateprefix

import (
	"encoding/binary"
	"sync"

	"github.com/llm-d/llm-d-router/pkg/common/clamp"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
)

// windowSlot is one sequenced element. A drop slot retracts dropGen.
// Empty hashes are a withdrawn prompt.
type windowSlot struct {
	hashes  []blockHash
	drop    bool
	dropGen uint64
}

// recentWindow buffers recently recorded prompt hash sequences per endpoint.
// Each inner slice is one prompt's hashes starting at block 0. Cap is on total
// hashes across prompts so cross-replica publish payloads stay small.
type recentWindow struct {
	mu      sync.Mutex
	pending map[string][]windowSlot
	// base is the generation of pending[endpointID][0]. It advances by one for
	// each element trimmed, so a generation always names the same element.
	base map[string]uint64
	max  int
}

func newRecentWindow(max int) *recentWindow {
	if max <= 0 {
		max = defaultLRUCapacityPerServer
	}
	return &recentWindow{
		pending: make(map[string][]windowSlot),
		base:    make(map[string]uint64),
		max:     max,
	}
}

// record appends one prompt's hashes for endpointID and trims oldest prompts
// until the total hash count is at most max. A single oversized prompt is
// head-capped to max so matching still anchors at block 0.
func (w *recentWindow) record(endpointID string, hashes []blockHash) {
	if len(hashes) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	prompt := hashes
	if len(prompt) > w.max {
		prompt = prompt[:w.max]
	}
	cp := make([]blockHash, len(prompt))
	copy(cp, prompt)

	buf := w.pending[endpointID]
	buf = append(buf, windowSlot{hashes: cp})
	trimmed, dropped := trimSlots(buf, w.max)
	w.pending[endpointID] = trimmed
	w.base[endpointID] += clamp.Uint64(dropped)
}

// snapshot returns a deep copy of pending prompts for endpointID without clearing.
// Peers read the window as a whole on every publish, so draining would wipe
// peer-visible hashes on the next idle publish tick.
func (w *recentWindow) snapshot(endpointID string) [][]blockHash {
	prompts, _ := w.snapshotWindow(endpointID)
	live := prompts[:0:0]
	for _, p := range prompts {
		if len(p) > 0 {
			live = append(live, p)
		}
	}
	if len(live) == 0 {
		return nil
	}
	return live
}

// snapshotWindow returns content slots, including empty withdrawn prompts, and
// the generation of the first element. Retraction slots are omitted.
func (w *recentWindow) snapshotWindow(endpointID string) ([][]blockHash, uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var prompts [][]blockHash
	for _, slot := range w.pending[endpointID] {
		if slot.drop {
			continue
		}
		prompts = append(prompts, append([]blockHash(nil), slot.hashes...))
	}
	return prompts, w.base[endpointID]
}

// sequenced is the value incremental replication publishes for endpointID.
func (w *recentWindow) sequenced(endpointID string) *attrprefix.PrefixHashSet {
	w.mu.Lock()
	defer w.mu.Unlock()
	slots := w.pending[endpointID]
	set := &attrprefix.PrefixHashSet{Base: w.base[endpointID]}
	for _, slot := range slots {
		if slot.drop {
			set.AppendRetract(slot.dropGen)
			continue
		}
		set.AppendContent(hashesToUint64(slot.hashes))
	}
	return set
}

// forget drops the buffer for endpointID when the endpoint leaves the pool.
func (w *recentWindow) forget(endpointID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.pending, endpointID)
	delete(w.base, endpointID)
}

// removePrompt empties a matching prompt and appends a retraction of its
// generation, then trims. Hashes use the same head cap as record. A prompt
// already trimmed out is a no-op.
func (w *recentWindow) removePrompt(endpointID string, hashes []blockHash) {
	if len(hashes) == 0 {
		return
	}
	prompt := hashes
	if len(prompt) > w.max {
		prompt = prompt[:w.max]
	}
	key := promptKey(prompt)
	w.mu.Lock()
	defer w.mu.Unlock()
	buf := w.pending[endpointID]
	n := len(buf)
	matched := false
	for i := 0; i < n; i++ {
		if buf[i].drop || promptKey(buf[i].hashes) != key {
			continue
		}
		gen := w.base[endpointID] + uint64(i)
		buf[i].hashes = nil
		buf = append(buf, windowSlot{drop: true, dropGen: gen})
		matched = true
	}
	if !matched {
		return
	}
	trimmed, dropped := trimSlots(buf, w.max)
	w.pending[endpointID] = trimmed
	w.base[endpointID] += clamp.Uint64(dropped)
}

// trimSlots drops oldest elements until the hash count and the slot count are
// each at most max, then drops a leading run of withdrawn elements. Those hold
// no hashes, so the hash cap alone does not bound them.
func trimSlots(slots []windowSlot, max int) ([]windowSlot, int) {
	if max <= 0 || len(slots) == 0 {
		return slots, 0
	}
	total := 0
	for _, slot := range slots {
		if !slot.drop {
			total += len(slot.hashes)
		}
	}
	dropped := 0
	dropFront := func() {
		if !slots[0].drop {
			total -= len(slots[0].hashes)
		}
		slots = slots[1:]
		dropped++
	}
	for total > max && len(slots) > 1 {
		dropFront()
	}
	for len(slots) > max {
		dropFront()
	}
	for len(slots) > 0 && withdrawnSlot(slots[0]) {
		dropFront()
	}
	out := make([]windowSlot, len(slots))
	copy(out, slots)
	return out, dropped
}

func withdrawnSlot(slot windowSlot) bool {
	return slot.drop || len(slot.hashes) == 0
}

func hashesToUint64(hashes []blockHash) []uint64 {
	if len(hashes) == 0 {
		return []uint64{}
	}
	out := make([]uint64, len(hashes))
	for i, h := range hashes {
		out[i] = uint64(h)
	}
	return out
}

func promptsToUint64(prompts [][]blockHash) [][]uint64 {
	if len(prompts) == 0 {
		return nil
	}
	out := make([][]uint64, len(prompts))
	for i, p := range prompts {
		cp := make([]uint64, len(p))
		for j, h := range p {
			cp[j] = uint64(h)
		}
		out[i] = cp
	}
	return out
}

func uint64sToBlockHashes(prompt []uint64) []blockHash {
	out := make([]blockHash, len(prompt))
	for i, h := range prompt {
		out[i] = blockHash(h)
	}
	return out
}

// promptKey identifies a prompt hash sequence for dedup. Length-prefixed so
// adjacent equal hashes from different prompts cannot collide.
func promptKey(hashes []blockHash) string {
	if len(hashes) == 0 {
		return ""
	}
	b := make([]byte, 8+8*len(hashes))
	binary.LittleEndian.PutUint64(b[:8], uint64(len(hashes)))
	for i, h := range hashes {
		binary.LittleEndian.PutUint64(b[8+8*i:], uint64(h))
	}
	return string(b)
}

// promptKeyUint64 is promptKey for sync-attribute prompts that are still
// []uint64, avoiding a blockHash conversion on the steady-state merge path.
func promptKeyUint64(hashes []uint64) string {
	if len(hashes) == 0 {
		return ""
	}
	b := make([]byte, 8+8*len(hashes))
	binary.LittleEndian.PutUint64(b[:8], uint64(len(hashes)))
	for i, h := range hashes {
		binary.LittleEndian.PutUint64(b[8+8*i:], h)
	}
	return string(b)
}
