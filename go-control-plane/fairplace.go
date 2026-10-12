package dovetail

// The interleaved fair-share placement loop.
//
// This replaced a version that computed the tenant ordering once per cycle and
// then placed each tenant's work to exhaustion in turn. That is not fair
// sharing, and the distinction is subtle enough that the code looked right:
// the lowest-share tenant goes first, which is correct, and then fills the
// entire cluster before the second tenant is considered at all. The ordering
// decided who got *everything* rather than who got *more*. A test with one
// tenant submitting 100 tasks and another submitting 2 gave the second tenant
// nothing.
//
// The fix is the loop Dominant Resource Fairness actually specifies: pick the
// tenant with the lowest weighted dominant share, give it **one** unit of
// work, then recompute shares and pick again. Placing one unit at a time is
// what lets a tenant's share rise during the cycle and hand the next slot to
// someone else.
//
// A "unit" is one task, or an entire gang. Gangs have to be the unit of
// placement rather than a special case inside it, because a gang's
// feasibility and its quota check are both properties of the whole set.

// unit is one indivisible placement attempt.
type unit struct {
	job   *Job
	tasks []*Task
	total Resources
	gang  bool
}

// tenantQueue is one tenant's pending work for this cycle.
type tenantQueue struct {
	tenant string
	units  []unit
	pos    int
	// remainingTotal is the unplaced demand in units[pos:], maintained
	// incrementally.
	//
	// This used to be recomputed by summing the slice on every iteration of
	// the placement loop, which is O(queue depth) per placement and therefore
	// quadratic per cycle. It was invisible at test scale and made the
	// simulator hang once a backlog of ~36,000 tasks built up. Keeping a
	// running total is the whole fix.
	remainingTotal Resources
	// blocked stops further attempts for this tenant in this cycle.
	//
	// Set when a tenant hits its quota or the cluster has no room. Neither
	// condition improves mid-cycle — capacity is only released by expiry,
	// which already ran, and quota headroom only grows when work finishes —
	// so continuing to retry is a guaranteed-identical no that turns the
	// placement loop quadratic in queue depth.
	blocked bool
}

func (q *tenantQueue) exhausted() bool { return q.blocked || q.pos >= len(q.units) }

func (q *tenantQueue) peek() (unit, bool) {
	if q.pos >= len(q.units) {
		return unit{}, false
	}
	return q.units[q.pos], true
}

// remaining is the tenant's unplaced demand, used for the share computation.
//
// Only counts units this cycle might still place. A blocked tenant reports
// zero so that OrderTenants drops it, which is how the loop terminates.
func (q *tenantQueue) remaining() Resources {
	if q.blocked {
		return Resources{}
	}
	return q.remainingTotal
}

// advance consumes the unit at the head of the queue.
//
// The only place pos moves, so the running total cannot drift out of step
// with it.
func (q *tenantQueue) advance() {
	if q.pos < len(q.units) {
		q.remainingTotal = q.remainingTotal.Sub(q.units[q.pos].total)
		q.pos++
	}
}

// placeFairLocked is step 3-4 of a scheduling cycle.
func (s *Scheduler) placeFairLocked(workers []*Worker, ctx SchedulingContext, result *CycleResult) {
	queues := s.buildQueuesLocked()
	if len(queues) == 0 {
		return
	}

	// Bound the loop independently of the termination logic. Every iteration
	// should either place a unit or block a tenant, so the natural bound is
	// units + tenants; the cap is generous slack against a future change
	// introducing a cycle. A scheduler that spins forever holding its own
	// lock is a worse failure than one that gives up on a cycle.
	maxIterations := 0
	for _, q := range queues {
		maxIterations += len(q.units)
	}
	maxIterations += len(queues) + 1

	for iteration := 0; iteration < maxIterations; iteration++ {
		// Recompute shares against the ledger as it stands *now*, which is
		// the whole point of this loop.
		usage := s.ledger.TenantUsage()
		demand := make(map[string]Resources, len(queues))
		for id, q := range queues {
			if r := q.remaining(); !r.IsZero() {
				demand[id] = r
			}
		}
		if len(demand) == 0 {
			return
		}

		order := OrderTenants(ComputeShares(s.tenants, usage, demand, ctx.ClusterTotal))

		placed := false
		for _, tenantID := range order {
			q := queues[tenantID]
			if q == nil || q.exhausted() {
				continue
			}
			if s.attemptUnitLocked(q, workers, ctx, result) {
				placed = true
			}
			// Break regardless of the outcome. On success the shares have
			// changed and the next slot may belong to someone else; on
			// failure the tenant is now blocked or has advanced past a dead
			// unit, and either way the ordering should be re-derived rather
			// than continuing down a stale list.
			break
		}

		if !placed && allExhausted(queues) {
			return
		}
	}
}

func allExhausted(queues map[string]*tenantQueue) bool {
	for _, q := range queues {
		if !q.exhausted() {
			return false
		}
	}
	return true
}

// attemptUnitLocked tries to place one tenant's next unit.
//
// Returns whether anything was placed. Always makes progress in one of three
// ways — places the unit, advances past a dead one, or blocks the tenant — so
// the caller's loop cannot spin.
func (s *Scheduler) attemptUnitLocked(
	q *tenantQueue,
	workers []*Worker,
	ctx SchedulingContext,
	result *CycleResult,
) bool {
	u, ok := q.peek()
	if !ok {
		return false
	}
	tenant, known := s.tenants[q.tenant]

	// Attempts are checked before quota: a task that can never run again
	// should be failed rather than counted against its tenant's quota and
	// reported as a capacity problem.
	for _, t := range u.tasks {
		if t.Attempts >= u.job.MaxAttempts {
			if u.gang {
				// A gang that can never be complete is not worth waiting
				// for, and leaving the rest pending forever is worse.
				for _, other := range u.tasks {
					other.Phase = PhaseFailed
					other.LastError = "gang member " + t.ID + " exhausted its attempts"
					result.Unplaced[other.ID] = ReasonAttemptsBurned
				}
				s.stats.TasksAbandoned += uint64(len(u.tasks))
			} else {
				t.Phase = PhaseFailed
				t.LastError = ReasonAttemptsBurned
				result.Unplaced[t.ID] = ReasonAttemptsBurned
				s.stats.TasksAbandoned++
			}
			q.advance()
			return false
		}
	}

	// Quota against the unit's total. For a gang that is the whole set:
	// admitting half a gang because half fits the headroom reproduces the
	// partial-placement deadlock through a different door.
	usage := s.ledger.TenantUsage()[q.tenant]
	if check := CheckQuota(tenant, known, usage, u.total); !check.Allowed {
		s.stats.QuotaRejections++
		for _, t := range u.tasks {
			result.Unplaced[t.ID] = check.Reason
		}
		// Blocked, not advanced. Headroom does not grow mid-cycle, and the
		// tenant's later units are no smaller in the dimension that is full.
		q.blocked = true
		s.markRemainingLocked(q, result, check.Reason)
		return false
	}

	if u.gang {
		plan := planGang(u.tasks, workers, s.ledger, ctx, q.tenant)
		if plan == nil {
			s.stats.GangsDeferred++
			for _, t := range u.tasks {
				result.Unplaced[t.ID] = ReasonGangInfeasible
			}
			// A gang that does not fit now blocks the tenant for this cycle
			// rather than letting its smaller units jump the queue. Letting
			// them through is how a large gang starves indefinitely behind a
			// stream of small work from its own tenant.
			q.blocked = true
			s.markRemainingLocked(q, result, ReasonGangInfeasible)
			return false
		}
		if !s.commitGangLocked(u, plan, workers, ctx, result) {
			q.blocked = true
			return false
		}
		q.advance()
		return true
	}

	task := u.tasks[0]
	filtered := FilterWorkers(workers, s.ledger, task.Request, ctx)
	if len(filtered.Feasible) == 0 {
		s.stats.Unplaceable++
		reason := filtered.DominantReason()
		if len(workers) == 0 {
			reason = ReasonNoWorkers
		}
		result.Unplaced[task.ID] = reason
		q.blocked = true
		s.markRemainingLocked(q, result, reason)
		return false
	}

	worker, score := SelectWorker(filtered.Feasible, s.ledger, task.Request, q.tenant, ctx)
	if worker == nil {
		result.Unplaced[task.ID] = ReasonInsufficient
		q.blocked = true
		return false
	}
	if err := s.commitLocked(task, worker, score, ctx.Now, result); err != nil {
		result.Unplaced[task.ID] = err.Error()
		q.advance()
		return false
	}

	// Keep per-tenant zone counts current within the cycle, or every task in
	// a burst sees the same zone as empty and they all land together.
	ctx.TenantZoneCount[q.tenant+"/"+worker.Topology.Zone]++
	q.advance()
	return true
}

// markRemainingLocked records a reason for a blocked tenant's unattempted
// units.
//
// Without this, a cycle reports a reason only for the one task that hit the
// wall and says nothing about the forty behind it, which reads to an operator
// as though those forty were never considered.
func (s *Scheduler) markRemainingLocked(q *tenantQueue, result *CycleResult, reason string) {
	for _, u := range q.units[q.pos:] {
		for _, t := range u.tasks {
			if _, already := result.Unplaced[t.ID]; !already {
				result.Unplaced[t.ID] = reason
			}
		}
	}
}

// commitGangLocked places every member of a planned gang, rolling back on any
// failure.
func (s *Scheduler) commitGangLocked(
	u unit,
	plan *GangPlan,
	workers []*Worker,
	ctx SchedulingContext,
	result *CycleResult,
) bool {
	byID := make(map[string]*Worker, len(workers))
	for _, w := range workers {
		byID[w.ID] = w
	}

	committed := make([]string, 0, len(u.tasks))
	for _, t := range u.tasks {
		worker := byID[plan.Placements[t.ID]]
		if worker == nil {
			// Every member was proven to fit against the scratch ledger, so
			// this means the real and planned states diverged — a bug, not a
			// capacity problem.
			s.rollbackLocked(committed)
			for _, x := range u.tasks {
				result.Unplaced[x.ID] = ReasonGangInfeasible
			}
			return false
		}
		if err := s.commitLocked(t, worker, plan.Scores[t.ID], ctx.Now, result); err != nil {
			s.rollbackLocked(committed)
			for _, x := range u.tasks {
				result.Unplaced[x.ID] = "gang commit failed: " + err.Error()
			}
			return false
		}
		committed = append(committed, t.ID)
		ctx.TenantZoneCount[u.job.Tenant+"/"+worker.Topology.Zone]++
	}
	return true
}

// buildQueuesLocked assembles per-tenant work queues for this cycle.
func (s *Scheduler) buildQueuesLocked() map[string]*tenantQueue {
	queues := make(map[string]*tenantQueue)

	for _, jobID := range sortedKeys(s.jobs) {
		job := s.jobs[jobID]
		if job.Complete() {
			continue
		}
		pending := job.Pending()
		if len(pending) == 0 {
			continue
		}

		q := queues[job.Tenant]
		if q == nil {
			q = &tenantQueue{tenant: job.Tenant}
			queues[job.Tenant] = q
		}

		if job.Gang {
			var total Resources
			for _, t := range pending {
				total = total.Add(t.Request)
			}
			q.units = append(q.units, unit{job: job, tasks: pending, total: total, gang: true})
			continue
		}
		for _, t := range pending {
			q.units = append(q.units, unit{job: job, tasks: []*Task{t}, total: t.Request})
		}
	}

	// Bound how deep a single cycle looks.
	//
	// A cycle cannot place more than the cluster has room for, so examining a
	// 36,000-deep backlog every 250ms is pure waste — and it was enough waste
	// to make the simulator appear to hang. Truncating after the priority sort
	// keeps the head of each queue, which is the part a cycle could actually
	// have placed.
	//
	// This does change behaviour under extreme backlog: a tenant with 36,000
	// queued tasks is considered for its first MaxUnitsPerCycle only. That is
	// correct for throughput and it does mean the *reported* unplaced reasons
	// cover a prefix of the queue rather than all of it, which is noted in
	// the README.
	limit := s.config.MaxUnitsPerCycle
	for _, q := range queues {
		sortUnits(q.units)
		if limit > 0 && len(q.units) > limit {
			q.units = q.units[:limit]
		}
		for _, u := range q.units {
			q.remainingTotal = q.remainingTotal.Add(u.total)
		}
	}
	return queues
}

func sortUnits(units []unit) {
	// A stable insertion sort by (priority desc, submitted asc, job id, task
	// id). Hand-written rather than sort.Slice because the comparison needs
	// the job *and* the task identity to be fully deterministic, and the
	// queues are short.
	for i := 1; i < len(units); i++ {
		for j := i; j > 0 && unitLess(units[j], units[j-1]); j-- {
			units[j], units[j-1] = units[j-1], units[j]
		}
	}
}

func unitLess(a, b unit) bool {
	if a.job.Priority != b.job.Priority {
		return a.job.Priority > b.job.Priority
	}
	if !a.job.SubmittedAt.Equal(b.job.SubmittedAt) {
		return a.job.SubmittedAt.Before(b.job.SubmittedAt)
	}
	if a.job.ID != b.job.ID {
		return a.job.ID < b.job.ID
	}
	return a.tasks[0].ID < b.tasks[0].ID
}
