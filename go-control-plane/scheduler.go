package dovetail

import (
	"fmt"
	"sync"
	"time"
)

// Scheduler is the control plane.
//
// One cycle does five things in this order, and the order is load-bearing:
//
//  1. Expire — reclaim capacity from dead workers and lapsed reservations.
//  2. Snapshot — take one consistent view of the fleet for the whole cycle.
//  3. Order — rank tenants by fair share.
//  4. Place — walk tenants in that order, placing what quota allows.
//  5. Audit — assert the ledger's invariants still hold.
//
// Expiry comes first because capacity reclaimed from a dead worker should be
// usable in the same cycle that notices the death. My first version expired
// after placing, which meant a worker failure cost a full extra cycle of
// latency for no reason — the capacity was free and the scheduler had already
// decided it had none.
//
// The snapshot is taken once. A cycle that re-read the registry would watch
// capacity shift underneath its own decisions, and each placement would be
// individually justified while the set was collectively infeasible.
type Scheduler struct {
	mu       sync.Mutex
	registry *Registry
	ledger   *Ledger
	config   Config

	tenants map[string]Tenant
	jobs    map[string]*Job
	tasks   map[string]*Task

	// epoch increments per cycle and is stamped on assignments so a worker
	// replaying a stale command can be told to ignore it rather than starting
	// a second copy of a task.
	epoch uint64

	// Observability. Counters rather than a metrics library because the
	// simulator reads them directly and a dependency would buy nothing.
	stats Stats
}

// Stats are cumulative counters.
type Stats struct {
	Cycles             uint64
	Placed             uint64
	Unplaceable        uint64
	QuotaRejections    uint64
	ReservationsLapsed uint64
	WorkersLost        uint64
	TasksLost          uint64
	TasksRetried       uint64
	TasksAbandoned     uint64
	GangsDeferred      uint64
	ReconcileOrphans   uint64
	ReconcileGhosts    uint64
}

func NewScheduler(cfg Config) (*Scheduler, error) {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Scheduler{
		registry: NewRegistry(),
		ledger:   NewLedger(),
		config:   cfg,
		tenants:  make(map[string]Tenant),
		jobs:     make(map[string]*Job),
		tasks:    make(map[string]*Task),
	}, nil
}

func (s *Scheduler) Registry() *Registry { return s.registry }
func (s *Scheduler) Config() Config      { return s.config }

func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// AddTenant registers or updates a tenant.
func (s *Scheduler) AddTenant(t Tenant) error {
	if t.ID == "" {
		return fmt.Errorf("tenant id is empty")
	}
	if t.Quota.HasNegative() {
		return fmt.Errorf("tenant %s has a negative quota %s", t.ID, t.Quota)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenants[t.ID] = t
	return nil
}

// Submit queues a job.
//
// Validation happens here rather than at placement. A task asking for
// negative memory would otherwise sit in the queue being filtered out by
// every worker forever, and the operator would read "no suitable node" rather
// than "this request is nonsense".
//
// A job is also rejected if no single worker could ever run one of its tasks.
// That check needs the fleet, so it is advisory — the fleet can grow — but
// catching it at submission turns a permanently pending task into an
// immediate error, which is the difference between a bug report and a
// mystery.
func (s *Scheduler) Submit(j *Job, now time.Time) error {
	if j.ID == "" {
		return fmt.Errorf("job id is empty")
	}
	if len(j.Tasks) == 0 {
		return fmt.Errorf("job %s has no tasks", j.ID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[j.ID]; exists {
		return fmt.Errorf("job %s already submitted", j.ID)
	}
	if _, known := s.tenants[j.Tenant]; !known {
		// Refused rather than given unlimited capacity. Treating a missing
		// tenant as infinite means a typo creates an unbounded tenant and the
		// cluster finds out under load.
		return fmt.Errorf("job %s names unknown tenant %q", j.ID, j.Tenant)
	}

	largestCapacity := s.largestWorkerCapacityLocked()
	for i, t := range j.Tasks {
		if t.ID == "" {
			return fmt.Errorf("job %s task %d has no id", j.ID, i)
		}
		if _, dup := s.tasks[t.ID]; dup {
			return fmt.Errorf("task %s already exists", t.ID)
		}
		if err := t.Request.Validate(); err != nil {
			return fmt.Errorf("job %s task %s: %w", j.ID, t.ID, err)
		}
		if !largestCapacity.IsZero() && !t.Request.Fits(largestCapacity) {
			return fmt.Errorf("job %s task %s requests %s, which exceeds the largest worker (%s)",
				j.ID, t.ID, t.Request, largestCapacity)
		}
		t.JobID = j.ID
		t.Tenant = j.Tenant
		t.Phase = PhasePending
	}

	j.SubmittedAt = now
	if j.MaxAttempts <= 0 {
		j.MaxAttempts = s.config.MaxAttempts
	}
	s.jobs[j.ID] = j
	for _, t := range j.Tasks {
		s.tasks[t.ID] = t
	}
	return nil
}

func (s *Scheduler) largestWorkerCapacityLocked() Resources {
	var largest Resources
	for _, w := range s.registry.Snapshot() {
		if largest.Fits(w.Capacity) {
			largest = w.Capacity
		}
	}
	return largest
}

// CycleResult is what one scheduling pass did.
type CycleResult struct {
	Epoch     uint64
	Decisions []Decision
	// Unplaced carries a reason per task, which is the thing an operator
	// actually needs. A cycle that placed nothing and says nothing is
	// unoperable.
	Unplaced map[string]string
	Expired  []Assignment
	Lost     []string
}

// Cycle runs one scheduling pass.
func (s *Scheduler) Cycle(now time.Time) (CycleResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.epoch++
	s.stats.Cycles++
	result := CycleResult{Epoch: s.epoch, Unplaced: make(map[string]string)}

	// 1. Expire first, so capacity freed by a failure is usable immediately.
	result.Lost = s.reapStaleWorkersLocked(now)
	result.Expired = s.expireReservationsLocked(now)

	// 2. One snapshot for the whole cycle.
	workers := s.registry.Snapshot()
	clusterTotal := s.registry.ClusterCapacity(now, s.config.HeartbeatTimeout)

	ctx := SchedulingContext{
		Now:             now,
		Config:          s.config,
		ClusterTotal:    clusterTotal,
		TenantZoneCount: s.tenantZoneCountsLocked(workers),
	}

	// 3 & 4. Place, interleaving tenants by fair share.
	//
	// Recomputed after every placement rather than ordered once. Ordering
	// once was my first version and it does not implement fair sharing at
	// all: the lowest-share tenant goes first and then drains the cluster
	// inside the same cycle, so the ordering decides who gets everything
	// rather than who gets more. The test that caught it had one tenant
	// submit 100 tasks and another submit 2, and the second got nothing.
	s.placeFairLocked(workers, ctx, &result)

	// 5. Audit. Cheap relative to a cycle, and it catches the leak class that
	// is otherwise invisible until the cluster mysteriously stops scheduling.
	if err := s.ledger.checkInvariants(); err != nil {
		return result, fmt.Errorf("ledger invariant violated after cycle %d: %w", s.epoch, err)
	}
	for _, w := range workers {
		if avail := s.ledger.Available(w); avail.HasNegative() {
			return result, fmt.Errorf("worker %s is overcommitted after cycle %d: available %s",
				w.ID, s.epoch, avail)
		}
	}

	return result, nil
}

// commitLocked records an assignment and moves the task to Assigned.
func (s *Scheduler) commitLocked(task *Task, worker *Worker, score float64, now time.Time, result *CycleResult) error {
	a := Assignment{
		TaskID:    task.ID,
		WorkerID:  worker.ID,
		Tenant:    task.Tenant,
		Request:   task.Request,
		CreatedAt: now,
		Epoch:     s.epoch,
	}
	if err := s.ledger.Reserve(a); err != nil {
		return err
	}

	task.Phase = PhaseAssigned
	task.WorkerID = worker.ID
	task.AssignedAt = now
	task.Attempts++

	s.stats.Placed++
	result.Decisions = append(result.Decisions, Decision{
		TaskID:   task.ID,
		WorkerID: worker.ID,
		Tenant:   task.Tenant,
		Request:  task.Request,
		Score:    score,
	})
	return nil
}

// reapStaleWorkersLocked returns the assignments of workers that have gone
// quiet.
//
// Their tasks become Lost rather than Failed. The distinction is not
// pedantry: a task on an unreachable worker may well still be running, and
// "failed" would license an immediate retry that produces two live copies. A
// Lost task is requeued only because this scheduler has no fencing mechanism
// to prevent the duplicate — which is stated plainly in the README rather
// than papered over here.
func (s *Scheduler) reapStaleWorkersLocked(now time.Time) []string {
	var lost []string
	for _, w := range s.registry.Stale(now, s.config.HeartbeatTimeout) {
		assignments := s.ledger.ReleaseWorker(w.ID)
		s.registry.Remove(w.ID)
		s.stats.WorkersLost++
		lost = append(lost, w.ID)

		for _, a := range assignments {
			task, ok := s.tasks[a.TaskID]
			if !ok {
				continue
			}
			s.stats.TasksLost++
			job := s.jobs[task.JobID]
			if job != nil && task.Attempts >= job.MaxAttempts {
				task.Phase = PhaseFailed
				task.LastError = fmt.Sprintf("worker %s lost; attempts exhausted", w.ID)
				s.stats.TasksAbandoned++
				continue
			}
			task.Phase = PhasePending
			task.WorkerID = ""
			task.LastError = fmt.Sprintf("worker %s stopped heartbeating", w.ID)
			s.stats.TasksRetried++
		}
	}
	return lost
}

// expireReservationsLocked reclaims capacity from assignments a healthy
// worker never confirmed.
//
// Separate from worker staleness and necessarily shorter. A worker can be
// heartbeating perfectly and still have dropped one assignment command;
// waiting a full heartbeat timeout to notice would strand that capacity for
// no reason.
func (s *Scheduler) expireReservationsLocked(now time.Time) []Assignment {
	expired := s.ledger.Expired(now, s.config.ReservationTimeout)
	for _, a := range expired {
		s.ledger.Release(a.TaskID)
		s.stats.ReservationsLapsed++

		task, ok := s.tasks[a.TaskID]
		if !ok {
			continue
		}
		job := s.jobs[task.JobID]
		if job != nil && task.Attempts >= job.MaxAttempts {
			task.Phase = PhaseFailed
			task.LastError = "reservation lapsed; attempts exhausted"
			s.stats.TasksAbandoned++
			continue
		}
		task.Phase = PhasePending
		task.WorkerID = ""
		task.LastError = fmt.Sprintf("worker %s never confirmed the assignment", a.WorkerID)
		s.stats.TasksRetried++
	}
	return expired
}

// TaskStarted is called when a worker confirms a task is running.
func (s *Scheduler) TaskStarted(taskID, workerID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ledger.Confirm(taskID, workerID); err != nil {
		return err
	}
	task, ok := s.tasks[taskID]
	if !ok {
		return fmt.Errorf("unknown task %s", taskID)
	}
	task.Phase = PhaseRunning
	task.StartedAt = now
	return nil
}

// TaskFinished is called when a worker reports a terminal outcome.
func (s *Scheduler) TaskFinished(taskID string, success bool, errMsg string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskID]
	if !ok {
		return fmt.Errorf("unknown task %s", taskID)
	}
	s.ledger.Release(taskID)

	if success {
		task.Phase = PhaseSucceeded
		task.EndedAt = now
		return nil
	}

	job := s.jobs[task.JobID]
	if job != nil && task.Attempts < job.MaxAttempts {
		task.Phase = PhasePending
		task.WorkerID = ""
		task.LastError = errMsg
		s.stats.TasksRetried++
		return nil
	}
	task.Phase = PhaseFailed
	task.EndedAt = now
	task.LastError = errMsg
	s.stats.TasksAbandoned++
	return nil
}

func (s *Scheduler) jobsForTenantLocked(tenantID string) []*Job {
	var out []*Job
	for _, id := range sortedKeys(s.jobs) {
		j := s.jobs[id]
		if j.Tenant == tenantID && !j.Complete() {
			out = append(out, j)
		}
	}
	// Higher priority first, then oldest first. Priority orders *within* a
	// tenant only — across tenants, fair share decides, which is what stops a
	// tenant from buying the whole cluster by marking everything urgent.
	sortJobs(out)
	return out
}

func (s *Scheduler) pendingDemandLocked() map[string]Resources {
	out := make(map[string]Resources)
	for _, j := range s.jobs {
		if j.Complete() {
			continue
		}
		for _, t := range j.Tasks {
			if t.Phase == PhasePending {
				out[j.Tenant] = out[j.Tenant].Add(t.Request)
			}
		}
	}
	return out
}

func (s *Scheduler) tenantZoneCountsLocked(workers []*Worker) map[string]int {
	zoneOf := make(map[string]string, len(workers))
	for _, w := range workers {
		zoneOf[w.ID] = w.Topology.Zone
	}
	out := make(map[string]int)
	for _, t := range s.tasks {
		if !t.Phase.Occupies() {
			continue
		}
		if zone, ok := zoneOf[t.WorkerID]; ok {
			out[t.Tenant+"/"+zone]++
		}
	}
	return out
}

// Task returns a copy of a task's state.
func (s *Scheduler) Task(id string) (Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return Task{}, false
	}
	return *t, true
}

// Job returns a copy of a job's state, tasks included.
func (s *Scheduler) Job(id string) (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return Job{}, false
	}
	copied := *j
	copied.Tasks = make([]*Task, len(j.Tasks))
	for i, t := range j.Tasks {
		tc := *t
		copied.Tasks[i] = &tc
	}
	return copied, true
}

// Shares returns the current fairness picture, for operators and the
// simulator.
func (s *Scheduler) Shares(now time.Time) []Share {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ComputeShares(
		s.tenants,
		s.ledger.TenantUsage(),
		s.pendingDemandLocked(),
		s.registry.ClusterCapacity(now, s.config.HeartbeatTimeout),
	)
}

// Utilization returns committed-over-capacity for the healthy fleet.
func (s *Scheduler) Utilization(now time.Time) (cpu, mem float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := s.registry.ClusterCapacity(now, s.config.HeartbeatTimeout)
	var committed Resources
	for _, r := range s.ledger.TenantUsage() {
		committed = committed.Add(r)
	}
	return committed.Utilization(total)
}

// AssignmentsOn returns the control plane's assignments on a worker.
//
// Exported for the simulator and for operator tooling. Returns copies, so a
// caller cannot mutate ledger state by holding one.
func (s *Scheduler) AssignmentsOn(workerID string) []Assignment {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ledger.AssignmentsOn(workerID)
}

// Available returns a worker's capacity that is genuinely free for new work.
//
// Reported free capacity minus unconfirmed reservations — the quantity from
// reservations.go, exposed because the Kubernetes extender has to answer
// "does this fit" using the same number the scheduler itself would use.
// Answering from worker.Reported instead would reproduce the double-booking
// bug one layer out: kube-scheduler would be told a node fits when this
// cycle has already promised the gap away.
//
// An unknown worker returns the zero vector, so nothing fits — which is the
// correct answer for capacity the control plane has no record of.
func (s *Scheduler) Available(workerID string) Resources {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.registry.Get(workerID)
	if !ok {
		return Resources{}
	}
	return s.ledger.Available(w)
}

// ScoreWorker returns the placement score a worker would receive for a
// request, and whether the worker is known.
//
// Exposed for the Kubernetes extender's prioritize call so that both
// schedulers rank nodes identically. A separate scoring implementation on the
// Kubernetes side would drift from this one, and the symptom would be
// placements that look wrong only when the extender is in the path.
func (s *Scheduler) ScoreWorker(workerID string, request Resources, tenant string,
	now time.Time) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, ok := s.registry.Get(workerID)
	if !ok {
		return 0, false
	}
	workers := s.registry.Snapshot()
	ctx := SchedulingContext{
		Now:             now,
		Config:          s.config,
		ClusterTotal:    s.registry.ClusterCapacity(now, s.config.HeartbeatTimeout),
		TenantZoneCount: s.tenantZoneCountsLocked(workers),
	}
	return Score(w, s.ledger, request, tenant, ctx), true
}
