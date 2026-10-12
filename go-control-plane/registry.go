package dovetail

import (
	"fmt"
	"sync"
	"time"
)

// Registry is the control plane's view of the fleet.
//
// Workers are the one piece of state that arrives from outside and can lie,
// go quiet, or come back claiming something different from what we last
// believed. The registry's job is to hold that view and to be precise about
// the three states a worker can be in, because conflating any two of them
// causes a specific bug I hit:
//
//	Warming   registered, no heartbeat yet. Not placeable — placing work on a
//	          machine that has never reported is placing it on faith.
//	Healthy   heartbeated within the timeout. Placeable.
//	Stale     gone quiet past the timeout. Its assignments are suspect.
//
// The bug: my first version had only healthy/unhealthy, so a worker was
// reaped during the gap between registering and its first heartbeat. Its
// (nonexistent) assignments were returned and it was counted as a failure,
// which made cold starts look like crashes and made the fleet appear to be
// flapping. `Warming` exists because of that.
type Registry struct {
	mu      sync.RWMutex
	workers map[string]*Worker
}

func NewRegistry() *Registry {
	return &Registry{workers: make(map[string]*Worker)}
}

// Register adds or re-adds a worker.
//
// Re-registration is the normal path for a restarted worker and is not an
// error. It resets the heartbeat to zero — back to warming — because a worker
// that just restarted is running nothing, and carrying its old heartbeat
// forward would make it immediately placeable while it is still booting.
func (r *Registry) Register(id string, capacity Resources, topo Topology, now time.Time) (*Worker, error) {
	if id == "" {
		return nil, fmt.Errorf("worker id is empty")
	}
	if capacity.HasNegative() || capacity.IsZero() {
		return nil, fmt.Errorf("worker %s registered with unusable capacity %s", id, capacity)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	w := &Worker{
		ID:           id,
		Capacity:     capacity,
		Reported:     capacity,
		Topology:     topo,
		RegisteredAt: now,
		// Explicitly zero: warming, not healthy. See the type comment.
		LastHeartbeat: time.Time{},
	}
	r.workers[id] = w
	copied := *w
	return &copied, nil
}

// Heartbeat records a worker's report of its own state.
//
// The worker reports free capacity and the task IDs it believes it is
// running. Both are taken at face value here and cross-checked by the
// reconciler — the registry's job is to record what was said, not to judge
// it, because the judgement needs the ledger and belongs in one place.
func (r *Registry) Heartbeat(id string, free Resources, runningTaskIDs []string, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, ok := r.workers[id]
	if !ok {
		// An unknown worker heartbeating is a real situation: the control
		// plane restarted and lost its registry while workers kept running.
		// Refused rather than implicitly registered, because the capacity is
		// unknown — the heartbeat carries *free* capacity, not total, and
		// inventing a total would corrupt every utilisation figure. The worker
		// re-registers on this error.
		return fmt.Errorf("unknown worker %s: re-register before heartbeating", id)
	}
	if free.HasNegative() {
		return fmt.Errorf("worker %s reported negative free capacity %s", id, free)
	}
	if !free.Fits(w.Capacity) {
		// A worker claiming more free capacity than it has is either
		// misconfigured or impersonating a bigger machine. Clamping would let
		// the scheduler overcommit it.
		return fmt.Errorf("worker %s reported %s free, which exceeds its capacity %s", id, free, w.Capacity)
	}

	w.Reported = free
	w.LastHeartbeat = now
	w.RunningTaskIDs = append(w.RunningTaskIDs[:0], runningTaskIDs...)
	return nil
}

// SetDraining marks a worker as accepting no new work.
func (r *Registry) SetDraining(id string, draining bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[id]
	if !ok {
		return fmt.Errorf("unknown worker %s", id)
	}
	w.Draining = draining
	return nil
}

// Remove deletes a worker outright.
func (r *Registry) Remove(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.workers[id]
	delete(r.workers, id)
	return ok
}

// Get returns a copy of a worker.
//
// A copy, not a pointer. Handing out a pointer into the registry lets a caller
// read a worker's capacity while a heartbeat mutates it, which is a data race
// the race detector finds immediately and a reviewer does not.
func (r *Registry) Get(id string) (*Worker, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w, ok := r.workers[id]
	if !ok {
		return nil, false
	}
	copied := *w
	copied.RunningTaskIDs = append([]string(nil), w.RunningTaskIDs...)
	return &copied, true
}

// Snapshot returns copies of every worker, in a deterministic order.
//
// The scheduler takes one snapshot per cycle and works from it. That is a
// deliberate choice over reading the registry live: a cycle that re-reads
// would see capacity change underneath its own decisions, and the resulting
// placements would be individually justified and collectively wrong.
func (r *Registry) Snapshot() []*Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Worker, 0, len(r.workers))
	for _, w := range r.workers {
		copied := *w
		copied.RunningTaskIDs = append([]string(nil), w.RunningTaskIDs...)
		out = append(out, &copied)
	}
	SortWorkersByID(out)
	return out
}

// Healthy returns the workers that can currently take work.
func (r *Registry) Healthy(now time.Time, timeout time.Duration) []*Worker {
	var out []*Worker
	for _, w := range r.Snapshot() {
		if w.Healthy(now, timeout) && !w.Draining {
			out = append(out, w)
		}
	}
	return out
}

// Stale returns workers that have gone quiet past the timeout.
func (r *Registry) Stale(now time.Time, timeout time.Duration) []*Worker {
	var out []*Worker
	for _, w := range r.Snapshot() {
		if w.Stale(now, timeout) {
			out = append(out, w)
		}
	}
	return out
}

// Warming returns workers that have registered but not yet reported.
//
// Exposed because it belongs in operator output. A fleet with many warming
// workers is mid-scale-up; one with many stale workers is mid-incident, and a
// single "unhealthy" count cannot tell the two apart.
func (r *Registry) Warming() []*Worker {
	var out []*Worker
	for _, w := range r.Snapshot() {
		if w.Warming() {
			out = append(out, w)
		}
	}
	return out
}

// ClusterCapacity sums the capacity of workers that can take work.
//
// Healthy workers only, which is the subtlety. Fair shares are fractions of
// *this* number, so including a dead worker's capacity would shrink every
// tenant's apparent share and make a shrinking cluster look increasingly fair
// while it became increasingly contended.
func (r *Registry) ClusterCapacity(now time.Time, timeout time.Duration) Resources {
	var total Resources
	for _, w := range r.Snapshot() {
		if w.Healthy(now, timeout) {
			total = total.Add(w.Capacity)
		}
	}
	return total
}

func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.workers)
}

// FleetStatus is a point-in-time summary for operators and the simulator.
type FleetStatus struct {
	Total    int
	Healthy  int
	Warming  int
	Stale    int
	Draining int
	Capacity Resources
	// Reported is the sum of free capacity across healthy workers, which is
	// an upper bound on what can be scheduled and not the real figure — the
	// real one subtracts unconfirmed reservations and needs the ledger.
	Reported Resources
}

func (r *Registry) Status(now time.Time, timeout time.Duration) FleetStatus {
	s := FleetStatus{}
	for _, w := range r.Snapshot() {
		s.Total++
		switch {
		case w.Warming():
			s.Warming++
		case w.Stale(now, timeout):
			s.Stale++
		default:
			s.Healthy++
			s.Capacity = s.Capacity.Add(w.Capacity)
			s.Reported = s.Reported.Add(w.Reported)
		}
		if w.Draining {
			s.Draining++
		}
	}
	return s
}
