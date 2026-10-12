package dovetail

import (
	"fmt"
	"time"
)

// Reconciliation compares what the control plane believes against what workers
// report, and fixes the difference.
//
// This file exists because of an assumption I started with and had to give up:
// that issuing an assignment and having it take effect are the same event.
// They are not. The command can be lost, the worker can restart between
// accepting and running it, the control plane can restart and forget, and a
// worker can be running something nobody remembers asking for. None of those
// are exotic — they are the normal consequence of two processes communicating
// over a network that drops things.
//
// So the scheduler does not assume its commands succeeded. It periodically
// compares desired state against reported state and classifies every
// disagreement into one of two kinds, which need opposite responses:
//
//	Orphan  the worker is running a task the control plane has no record of.
//	        Our bookkeeping is wrong, not the worker's. Adopt it — the work is
//	        real and consuming real capacity, and refusing to account for it
//	        means scheduling on top of it.
//
//	Ghost   the control plane thinks a task is running on a worker that does
//	        not report it. The worker is authoritative about its own
//	        processes, so release the capacity and requeue.
//
// Getting these backwards is tempting because "the control plane is the source
// of truth" sounds right. It is the source of truth about *intent*. The worker
// is the source of truth about *execution*, and reconciliation only works if
// each is believed about the thing it actually knows.

// Discrepancy is one disagreement found during reconciliation.
type Discrepancy struct {
	Kind     string // "orphan" or "ghost"
	TaskID   string
	WorkerID string
	Detail   string
}

const (
	DiscrepancyOrphan = "orphan"
	DiscrepancyGhost  = "ghost"
)

// ReconcileResult is what one pass found and did.
type ReconcileResult struct {
	Discrepancies []Discrepancy
	Adopted       int
	Released      int
}

// Reconcile compares the ledger against worker heartbeat reports.
//
// Only healthy workers are examined. A stale worker's assignments are handled
// by the staleness path in the scheduler, and reconciling against a worker
// that has not spoken recently would treat its silence as a report of nothing
// running — releasing every task on a machine that is merely slow to
// heartbeat.
func (s *Scheduler) Reconcile(now time.Time) ReconcileResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := ReconcileResult{}

	for _, w := range s.registry.Snapshot() {
		if !w.Healthy(now, s.config.HeartbeatTimeout) {
			continue
		}

		reported := make(map[string]bool, len(w.RunningTaskIDs))
		for _, id := range w.RunningTaskIDs {
			reported[id] = true
		}

		// Ghosts: we think it is here, the worker does not report it.
		for _, a := range s.ledger.AssignmentsOn(w.ID) {
			if reported[a.TaskID] {
				continue
			}
			if !a.Confirmed {
				// Never confirmed in the first place. This is a reservation
				// still inside its grace period, not a ghost — the worker may
				// simply not have started it yet. The reservation timeout owns
				// this case, and treating it as a ghost here would race that
				// timeout and release capacity a moment before the worker
				// reports the task running.
				continue
			}
			s.ledger.Release(a.TaskID)
			result.Released++
			s.stats.ReconcileGhosts++
			result.Discrepancies = append(result.Discrepancies, Discrepancy{
				Kind:     DiscrepancyGhost,
				TaskID:   a.TaskID,
				WorkerID: w.ID,
				Detail:   "previously confirmed running, no longer reported",
			})

			if task, ok := s.tasks[a.TaskID]; ok {
				job := s.jobs[task.JobID]
				if job != nil && task.Attempts >= job.MaxAttempts {
					task.Phase = PhaseFailed
					task.LastError = "vanished from its worker; attempts exhausted"
					s.stats.TasksAbandoned++
					continue
				}
				task.Phase = PhasePending
				task.WorkerID = ""
				task.LastError = fmt.Sprintf("worker %s stopped reporting the task", w.ID)
				s.stats.TasksRetried++
			}
		}

		// Orphans: the worker reports it, we have no record.
		for _, taskID := range w.RunningTaskIDs {
			if _, tracked := s.ledger.Get(taskID); tracked {
				continue
			}

			task, known := s.tasks[taskID]
			if !known {
				// A task from a previous control-plane lifetime, or from
				// another control plane entirely. We cannot adopt it: without
				// a record we do not know its resource request, and inventing
				// one would corrupt the accounting that reconciliation exists
				// to protect. Reported for an operator and otherwise left
				// alone — the worker's own free-capacity report already
				// accounts for it, so the scheduler will not overcommit.
				s.stats.ReconcileOrphans++
				result.Discrepancies = append(result.Discrepancies, Discrepancy{
					Kind:     DiscrepancyOrphan,
					TaskID:   taskID,
					WorkerID: w.ID,
					Detail:   "worker reports an unknown task; cannot adopt without a resource request",
				})
				continue
			}

			// A task we know about, running somewhere we did not expect.
			// Adopt it: the work is real and consuming real capacity.
			a := Assignment{
				TaskID:    taskID,
				WorkerID:  w.ID,
				Tenant:    task.Tenant,
				Request:   task.Request,
				Confirmed: true,
				CreatedAt: now,
				Epoch:     s.epoch,
			}
			if err := s.ledger.Reserve(a); err != nil {
				// Already assigned elsewhere. Two workers claiming the same
				// task is the one case where duplicate execution is certain
				// rather than suspected, and this scheduler has no fencing
				// token to resolve it — so it is reported loudly and left for
				// a human. Documented in the README as a real gap.
				result.Discrepancies = append(result.Discrepancies, Discrepancy{
					Kind:     DiscrepancyOrphan,
					TaskID:   taskID,
					WorkerID: w.ID,
					Detail:   "task appears to be running on two workers: " + err.Error(),
				})
				s.stats.ReconcileOrphans++
				continue
			}
			task.Phase = PhaseRunning
			task.WorkerID = w.ID
			result.Adopted++
			s.stats.ReconcileOrphans++
			result.Discrepancies = append(result.Discrepancies, Discrepancy{
				Kind:     DiscrepancyOrphan,
				TaskID:   taskID,
				WorkerID: w.ID,
				Detail:   "adopted a known task the control plane had lost track of",
			})
		}
	}

	return result
}

// AuditAccounting re-derives committed capacity from task state and compares
// it against the ledger.
//
// A second, independent check on the thing most likely to drift silently.
// Reconcile compares the control plane against workers; this compares the
// control plane against *itself* — the ledger against the task phases — which
// catches the class of bug where a task is marked finished but its reservation
// is never released. That leak is invisible from outside: the cluster simply
// reports less and less free capacity with nothing running on it, which is
// exactly the symptom that made me write this.
func (s *Scheduler) AuditAccounting() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ledger.checkInvariants(); err != nil {
		return err
	}

	// Every occupying task should hold exactly one assignment, and nothing
	// else should.
	for id, t := range s.tasks {
		a, held := s.ledger.Get(id)
		switch {
		case t.Phase.Occupies() && !held:
			return fmt.Errorf("task %s is %s but holds no reservation", id, t.Phase)
		case !t.Phase.Occupies() && held:
			return fmt.Errorf("task %s is %s but still holds a reservation on %s (leak)",
				id, t.Phase, a.WorkerID)
		case held && a.WorkerID != t.WorkerID:
			return fmt.Errorf("task %s records worker %s but is reserved on %s",
				id, t.WorkerID, a.WorkerID)
		}
	}

	// Per-tenant usage derived two ways must agree.
	fromLedger := s.ledger.TenantUsage()
	fromTasks := make(map[string]Resources)
	for _, t := range s.tasks {
		if t.Phase.Occupies() {
			fromTasks[t.Tenant] = fromTasks[t.Tenant].Add(t.Request)
		}
	}
	for _, tenant := range sortedKeys(fromLedger) {
		if fromLedger[tenant] != fromTasks[tenant] {
			return fmt.Errorf("tenant %s usage disagrees: ledger %s, tasks %s",
				tenant, fromLedger[tenant], fromTasks[tenant])
		}
	}
	for _, tenant := range sortedKeys(fromTasks) {
		if fromLedger[tenant] != fromTasks[tenant] {
			return fmt.Errorf("tenant %s usage disagrees: ledger %s, tasks %s",
				tenant, fromLedger[tenant], fromTasks[tenant])
		}
	}
	return nil
}
