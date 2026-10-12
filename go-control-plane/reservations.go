package dovetail

import (
	"fmt"
	"time"
)

// Ledger tracks the gap between what workers say they have free and what the
// control plane has already promised away.
//
// This file exists because of one bug. The first version of the scheduler read
// `worker.Reported` — the free capacity from the worker's last heartbeat — and
// placed work into it. Within a single scheduling cycle it would place two
// tasks into the same 4 GiB gap, because the heartbeat that reported the gap
// predated both decisions. The second task would then fail to start, be
// retried, and usually land in the same place again.
//
// The fix is to name three quantities instead of two and never confuse them:
//
//	Capacity  the machine's total. Static, from registration.
//	Reported  free capacity per the last heartbeat. Already accounts for
//	          everything the worker knows it is running.
//	Reserved  capacity the control plane has committed since — assignments
//	          the worker has not yet confirmed running.
//
//	Available = Reported - Reserved
//
// The subtlety is the word *unconfirmed*. Once a heartbeat reports a task
// running, the worker's own `Reported` figure accounts for it, and continuing
// to subtract our reservation would double-count: the capacity would appear
// consumed twice and the worker would look full at half utilisation. So a
// reservation is subtracted only while it is unconfirmed, and confirmation is
// precisely the moment the accounting responsibility transfers from the
// control plane to the worker's own report.
//
// Ledger is not safe for concurrent use. The scheduler owns it under its own
// lock, which is deliberate: a lock in here would permit two scheduling
// cycles to interleave and each take a consistent-looking snapshot of a
// ledger the other was mutating.
type Ledger struct {
	// byTask is the authoritative record, keyed by task.
	byTask map[string]*Assignment
	// byWorker indexes the same assignments for the per-worker sums the
	// placement filter needs on every candidate evaluation.
	byWorker map[string]map[string]*Assignment
}

func NewLedger() *Ledger {
	return &Ledger{
		byTask:   make(map[string]*Assignment),
		byWorker: make(map[string]map[string]*Assignment),
	}
}

// Reserve records a commitment of capacity on a worker.
//
// Returns an error rather than overwriting if the task is already assigned.
// Overwriting would leak the previous reservation — the capacity would stay
// subtracted from a worker nobody was tracking — and a double-assign is a
// scheduler bug worth surfacing loudly rather than absorbing.
func (l *Ledger) Reserve(a Assignment) error {
	if existing, ok := l.byTask[a.TaskID]; ok {
		return fmt.Errorf("task %s is already assigned to worker %s (epoch %d)",
			a.TaskID, existing.WorkerID, existing.Epoch)
	}
	copied := a
	l.byTask[a.TaskID] = &copied
	if l.byWorker[a.WorkerID] == nil {
		l.byWorker[a.WorkerID] = make(map[string]*Assignment)
	}
	l.byWorker[a.WorkerID][a.TaskID] = &copied
	return nil
}

// Confirm marks an assignment as reported-running by its worker.
//
// After this the reservation stops being subtracted from the worker's
// available capacity, because the worker's own heartbeat now accounts for it.
// Idempotent: a worker reports its full running set on every heartbeat, so
// confirming an already-confirmed task is the normal case and not an error.
func (l *Ledger) Confirm(taskID, workerID string) error {
	a, ok := l.byTask[taskID]
	if !ok {
		// The worker is running something we have no record of. Not an error
		// here — the reconciler decides what to do about it, because the right
		// answer depends on whether we ever knew about this task.
		return fmt.Errorf("no assignment for task %s", taskID)
	}
	if a.WorkerID != workerID {
		// A task confirmed by a worker other than the one it was assigned to.
		// This is the dangerous case: two copies may be running. Reported
		// rather than silently re-pointed.
		return fmt.Errorf("task %s is assigned to %s but was confirmed by %s",
			taskID, a.WorkerID, workerID)
	}
	a.Confirmed = true
	return nil
}

// Release removes an assignment, returning its capacity.
func (l *Ledger) Release(taskID string) (Assignment, bool) {
	a, ok := l.byTask[taskID]
	if !ok {
		return Assignment{}, false
	}
	delete(l.byTask, taskID)
	if m := l.byWorker[a.WorkerID]; m != nil {
		delete(m, taskID)
		if len(m) == 0 {
			// Drop the empty inner map. Without this, byWorker grows without
			// bound across a long run as workers come and go — a slow leak
			// that a short test never shows.
			delete(l.byWorker, a.WorkerID)
		}
	}
	return *a, true
}

// ReleaseWorker removes every assignment on a worker, for when it is lost.
func (l *Ledger) ReleaseWorker(workerID string) []Assignment {
	m := l.byWorker[workerID]
	if m == nil {
		return nil
	}
	out := make([]Assignment, 0, len(m))
	for taskID, a := range m {
		out = append(out, *a)
		delete(l.byTask, taskID)
	}
	delete(l.byWorker, workerID)
	return out
}

// Get returns the assignment for a task.
func (l *Ledger) Get(taskID string) (Assignment, bool) {
	a, ok := l.byTask[taskID]
	if !ok {
		return Assignment{}, false
	}
	return *a, true
}

// Unconfirmed sums the reservations on a worker that no heartbeat has yet
// confirmed.
//
// This is the quantity that must be subtracted from a worker's reported free
// capacity, and the reason the subtraction is not simply "all assignments".
func (l *Ledger) Unconfirmed(workerID string) Resources {
	var total Resources
	for _, a := range l.byWorker[workerID] {
		if !a.Confirmed {
			total = total.Add(a.Request)
		}
	}
	return total
}

// Committed sums every assignment on a worker, confirmed or not.
//
// Used for operator-facing reporting and for the reconciler's cross-check,
// never for placement. Placement that subtracted this from Reported would
// double-count confirmed work.
func (l *Ledger) Committed(workerID string) Resources {
	var total Resources
	for _, a := range l.byWorker[workerID] {
		total = total.Add(a.Request)
	}
	return total
}

// Available returns the capacity a worker really has free for new work.
//
// The one function in this file that placement should call.
func (l *Ledger) Available(w *Worker) Resources {
	return w.Reported.Sub(l.Unconfirmed(w.ID))
}

// TenantUsage sums every tenant's committed resources across the cluster.
//
// Counts both confirmed and unconfirmed, because a tenant that has just been
// given half the cluster is using half the cluster for fairness purposes even
// if no worker has confirmed it yet. Fairness that ignored unconfirmed
// assignments would hand the same tenant another half a cycle later.
func (l *Ledger) TenantUsage() map[string]Resources {
	out := make(map[string]Resources)
	for _, a := range l.byTask {
		out[a.Tenant] = out[a.Tenant].Add(a.Request)
	}
	return out
}

// Expired returns assignments that have been unconfirmed for longer than
// timeout.
//
// A worker can be perfectly healthy — heartbeating on time — and still have
// lost a particular assignment command. Waiting a full heartbeat timeout for
// that would strand the capacity for no reason, which is why the reservation
// timeout is separate and shorter.
func (l *Ledger) Expired(now time.Time, timeout time.Duration) []Assignment {
	var out []Assignment
	for _, a := range l.byTask {
		if !a.Confirmed && now.Sub(a.CreatedAt) > timeout {
			out = append(out, *a)
		}
	}
	return out
}

// AssignmentsOn returns every assignment on a worker.
func (l *Ledger) AssignmentsOn(workerID string) []Assignment {
	m := l.byWorker[workerID]
	out := make([]Assignment, 0, len(m))
	for _, a := range m {
		out = append(out, *a)
	}
	return out
}

func (l *Ledger) Len() int { return len(l.byTask) }

// checkInvariants verifies the ledger's internal consistency.
//
// Called from tests and from the scheduler's own audit path rather than on
// every mutation. The indices must agree exactly: a task present in byTask
// but missing from byWorker is capacity the scheduler has promised and will
// never reclaim, and that is a leak no amount of staring at placement code
// reveals.
func (l *Ledger) checkInvariants() error {
	indexed := 0
	for workerID, m := range l.byWorker {
		if len(m) == 0 {
			return fmt.Errorf("worker %s has an empty assignment map that should have been dropped", workerID)
		}
		for taskID, a := range m {
			indexed++
			primary, ok := l.byTask[taskID]
			if !ok {
				return fmt.Errorf("task %s is indexed under worker %s but absent from byTask", taskID, workerID)
			}
			if primary != a {
				return fmt.Errorf("task %s has diverging records in the two indices", taskID)
			}
			if a.WorkerID != workerID {
				return fmt.Errorf("task %s is indexed under worker %s but claims worker %s",
					taskID, workerID, a.WorkerID)
			}
		}
	}
	if indexed != len(l.byTask) {
		return fmt.Errorf("byWorker indexes %d assignments, byTask holds %d", indexed, len(l.byTask))
	}
	return nil
}
