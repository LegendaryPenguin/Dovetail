package dovetail

import (
	"sort"
	"time"
)

// Gang scheduling: all of a job's tasks start together, or none do.
//
// The reason this needs its own file is that a gang is not a preference, and
// the obvious implementation — place each task and hope — produces a specific
// and nasty failure. Half a gang lands, holds capacity, and makes no progress
// because the other half cannot be placed. Two such jobs can hold each other
// off indefinitely while the cluster appears busy and nothing completes. That
// is a deadlock with extra steps, and no amount of retrying fixes it because
// retrying is what caused it.
//
// So feasibility is decided for the whole set *before* any of it is committed,
// against a simulated ledger. If the whole gang does not fit, nothing is
// reserved and the job waits.
//
// What this does not implement is preemption. A gang that cannot fit waits for
// natural completions rather than evicting smaller work to make room, which
// means a large gang can be starved indefinitely by a steady stream of small
// tasks. That is a real limitation and it is in the README rather than hidden
// here — fixing it properly needs a reservation-with-backfill scheme, which is
// a different project.

// GangPlan is a feasible all-or-nothing placement.
type GangPlan struct {
	// Placements maps task ID to worker ID. Complete or the plan is nil.
	Placements map[string]string
	Scores     map[string]float64
}

// planGang decides whether an entire set of tasks can be placed right now.
//
// Works against a scratch copy of the reservation state so that a failed
// attempt leaves nothing behind. The first version mutated the real ledger and
// rolled back on failure, which worked until a rollback was missed on an early
// return and capacity leaked permanently — the cluster slowly reported less
// and less free space with nothing running on it.
func planGang(
	tasks []*Task,
	workers []*Worker,
	ledger *Ledger,
	ctx SchedulingContext,
	tenant string,
) *GangPlan {
	if len(tasks) == 0 {
		return nil
	}

	// Scratch capacity per worker, seeded from the real availability.
	scratch := make(map[string]Resources, len(workers))
	for _, w := range workers {
		if w.Draining || !w.Healthy(ctx.Now, ctx.Config.HeartbeatTimeout) {
			continue
		}
		scratch[w.ID] = ledger.Available(w)
	}
	if len(scratch) == 0 {
		return nil
	}

	// Largest tasks first. A gang of mixed sizes placed smallest-first
	// fragments the cluster and then cannot fit its own largest member, even
	// though an ordering existed that would have worked. This is only a
	// heuristic — it is first-fit-decreasing, not optimal, and bin packing
	// being NP-hard means a genuinely optimal placement is not on the table.
	ordered := make([]*Task, len(tasks))
	copy(ordered, tasks)
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i].Request, ordered[j].Request
		if a.MilliCPU != b.MilliCPU {
			return a.MilliCPU > b.MilliCPU
		}
		if a.MemBytes != b.MemBytes {
			return a.MemBytes > b.MemBytes
		}
		return ordered[i].ID < ordered[j].ID
	})

	plan := &GangPlan{
		Placements: make(map[string]string, len(tasks)),
		Scores:     make(map[string]float64, len(tasks)),
	}
	zoneUsed := make(map[string]int)

	for _, task := range ordered {
		// Candidate workers in a deterministic order, best-scoring first.
		type cand struct {
			id    string
			score float64
		}
		var cands []cand
		for _, w := range workers {
			avail, ok := scratch[w.ID]
			if !ok || !task.Request.Fits(avail) {
				continue
			}
			score := gangScore(w, avail, task.Request, zoneUsed, ctx)
			cands = append(cands, cand{w.ID, score})
		}
		if len(cands) == 0 {
			// One unplaceable member means the whole gang waits. Returning nil
			// rather than a partial plan is the entire point of this function.
			return nil
		}
		sort.Slice(cands, func(i, j int) bool {
			if cands[i].score != cands[j].score {
				return cands[i].score > cands[j].score
			}
			return cands[i].id < cands[j].id
		})

		chosen := cands[0]
		scratch[chosen.id] = scratch[chosen.id].Sub(task.Request)
		plan.Placements[task.ID] = chosen.id
		plan.Scores[task.ID] = chosen.score
		for _, w := range workers {
			if w.ID == chosen.id {
				zoneUsed[w.Topology.Zone]++
				break
			}
		}
	}
	return plan
}

// gangScore ranks a worker for one gang member.
//
// Differs from the general scorer in one way: when SpreadGangsAcrossZones is
// set, a zone already holding part of this gang is penalised. A gang confined
// to one zone is a gang that one zone failure kills entirely, which for
// all-or-nothing work means losing the whole job rather than a fraction of it.
// The trade is network locality, which for tightly-coupled work is exactly
// what a gang wants — so this is off by default and the simulator measures
// both.
func gangScore(w *Worker, available, request Resources, zoneUsed map[string]int, ctx SchedulingContext) float64 {
	after := w.Capacity.Sub(available.Sub(request))
	packCPU, packMem := after.Utilization(w.Capacity)
	pack := packCPU
	if packMem > pack {
		pack = packMem
	}

	if !ctx.Config.SpreadGangsAcrossZones {
		// Pack by default: co-located gang members talk to each other.
		return pack
	}
	penalty := float64(zoneUsed[w.Topology.Zone])
	return pack - 0.5*penalty
}

// placeGangLocked attempts an atomic placement of a job's pending tasks.
func (s *Scheduler) placeGangLocked(
	job *Job,
	pending []*Task,
	workers []*Worker,
	ctx SchedulingContext,
	result *CycleResult,
) {
	tenant, known := s.tenants[job.Tenant]

	// Quota is checked against the gang's *total*, not per task. Admitting
	// half a gang because half of it fits the quota headroom would reproduce
	// the partial-placement deadlock through a different door.
	var total Resources
	for _, t := range pending {
		total = total.Add(t.Request)
	}
	usage := s.ledger.TenantUsage()[job.Tenant]
	if check := CheckQuota(tenant, known, usage, total); !check.Allowed {
		s.stats.QuotaRejections++
		for _, t := range pending {
			result.Unplaced[t.ID] = check.Reason
		}
		return
	}

	for _, t := range pending {
		if t.Attempts >= job.MaxAttempts {
			// One member out of attempts fails the whole gang. A gang that
			// can never be complete is not a gang worth waiting for, and
			// leaving the rest pending forever is the worse outcome.
			for _, other := range pending {
				other.Phase = PhaseFailed
				other.LastError = "gang member " + t.ID + " exhausted its attempts"
			}
			s.stats.TasksAbandoned += uint64(len(pending))
			result.Unplaced[t.ID] = ReasonAttemptsBurned
			return
		}
	}

	plan := planGang(pending, workers, s.ledger, ctx, job.Tenant)
	if plan == nil {
		s.stats.GangsDeferred++
		for _, t := range pending {
			result.Unplaced[t.ID] = ReasonGangInfeasible
		}
		return
	}

	// Commit. Every member was proven to fit against the scratch ledger, so a
	// Reserve failure here means the real and scratch states had diverged,
	// which is a bug rather than a capacity problem — hence the rollback and
	// the loud reason.
	committed := make([]string, 0, len(pending))
	for _, t := range pending {
		workerID := plan.Placements[t.ID]
		var worker *Worker
		for _, w := range workers {
			if w.ID == workerID {
				worker = w
				break
			}
		}
		if worker == nil {
			s.rollbackLocked(committed)
			for _, x := range pending {
				result.Unplaced[x.ID] = ReasonGangInfeasible
			}
			return
		}
		if err := s.commitLocked(t, worker, plan.Scores[t.ID], ctx.Now, result); err != nil {
			s.rollbackLocked(committed)
			for _, x := range pending {
				result.Unplaced[x.ID] = "gang commit failed: " + err.Error()
			}
			return
		}
		committed = append(committed, t.ID)
	}
}

// rollbackLocked undoes a partial gang commit.
func (s *Scheduler) rollbackLocked(taskIDs []string) {
	for _, id := range taskIDs {
		s.ledger.Release(id)
		if t, ok := s.tasks[id]; ok {
			t.Phase = PhasePending
			t.WorkerID = ""
			// Undo the attempt increment: a rolled-back placement never
			// happened, and charging the task for it would burn its retries
			// on scheduler bugs rather than on real failures.
			if t.Attempts > 0 {
				t.Attempts--
			}
		}
		// The decision list in the result is left alone deliberately: it
		// records what the cycle attempted, and the caller distinguishes
		// attempted from durable via the Unplaced map.
	}
	if s.stats.Placed >= uint64(len(taskIDs)) {
		s.stats.Placed -= uint64(len(taskIDs))
	}
}

// GangWaitTime reports how long a gang job has been waiting, for the
// starvation metric the simulator reports.
//
// Worth measuring because the no-preemption limitation means this number can
// grow without bound, and a scheduler whose worst case is unbounded should at
// least be able to tell you when it is happening.
func (s *Scheduler) GangWaitTime(jobID string, now time.Time) (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok || !j.Gang {
		return 0, false
	}
	for _, t := range j.Tasks {
		if t.Phase == PhasePending {
			return now.Sub(j.SubmittedAt), true
		}
	}
	return 0, false
}

func sortJobs(jobs []*Job) {
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].Priority != jobs[j].Priority {
			return jobs[i].Priority > jobs[j].Priority
		}
		if !jobs[i].SubmittedAt.Equal(jobs[j].SubmittedAt) {
			return jobs[i].SubmittedAt.Before(jobs[j].SubmittedAt)
		}
		return jobs[i].ID < jobs[j].ID
	})
}
