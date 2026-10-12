// Package simulator drives the real scheduler against synthetic tenants.
//
// It exists because "the workloads eventually ran" is not an evaluation. The
// questions worth answering about a multi-tenant scheduler are comparative —
// does quota actually protect a small tenant, does packing bias change
// utilisation, what does a worker failure cost — and none of them can be
// answered by watching a cluster work correctly.
//
// Two properties make the results meaningful:
//
//   - **The scheduler is the real one.** Nothing here reimplements placement.
//     The simulator supplies a fleet, a clock and a workload, and everything
//     else is the same code the control plane runs.
//
//   - **The clock is virtual.** Every timestamp is computed, so a 2-hour
//     simulation runs in milliseconds and a run is exactly reproducible from
//     its seed. Sleeping would make the failure experiments take as long as
//     the failures do, which means they would not get run.
//
// What it is not: a model of real workloads. Task durations are drawn from a
// distribution I chose, not measured from anything, so absolute numbers
// describe the generator. The comparisons between configurations are the
// output worth reading.
package simulator

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	dovetail "github.com/LegendaryPenguin/dovetail"
)

// TenantProfile describes one synthetic tenant's behaviour.
type TenantProfile struct {
	ID     string
	Quota  dovetail.Resources
	Weight float64

	// SubmitRate is jobs per simulated second.
	SubmitRate float64
	// TasksPerJob is sampled uniformly from [Min, Max].
	TasksPerJobMin int
	TasksPerJobMax int
	// Request is the per-task resource ask.
	Request dovetail.Resources
	// DurationMean is the mean task runtime; actual durations are drawn from
	// an exponential distribution, which is heavy-tailed enough to produce
	// the stragglers that make packing decisions matter.
	DurationMean time.Duration
	// GangFraction of jobs are gangs.
	GangFraction float64
}

// Scenario is a complete experiment.
type Scenario struct {
	Name           string
	Workers        int
	WorkerCapacity dovetail.Resources
	Zones          []string
	Tenants        []TenantProfile
	Duration       time.Duration
	// CycleInterval is how often the scheduler runs. Shorter means lower
	// queue latency and more CPU spent scheduling.
	CycleInterval time.Duration
	Config        dovetail.Config
	Seed          int64

	// KillWorkerAt, when non-zero, removes a worker partway through, to
	// measure what a failure costs rather than only that it is survived.
	KillWorkerAt time.Duration
	// StallWorkerAt stops a worker heartbeating without removing it, which is
	// the more realistic failure: the control plane has to notice.
	StallWorkerAt time.Duration
}

// Result is what a scenario measured.
type Result struct {
	Scenario string

	TasksSubmitted int
	TasksCompleted int
	TasksFailed    int

	// QueueLatency is submission to first placement, per task.
	QueueLatencyP50 time.Duration
	QueueLatencyP95 time.Duration
	QueueLatencyMax time.Duration

	// Utilization is the mean over cycles of committed/capacity.
	MeanCPUUtil float64
	MeanMemUtil float64
	PeakCPUUtil float64

	// FairnessIndex is Jain's index over tenant dominant shares, averaged
	// over cycles. Read alongside the per-tenant completion counts, never
	// instead: it measures equality of allocation, not of satisfaction.
	MeanFairness float64

	PerTenantCompleted map[string]int
	PerTenantQueueP95  map[string]time.Duration

	Stats dovetail.Stats

	// AccountingErrors counts cycles where the ledger audit failed. Any
	// non-zero value invalidates the whole run, so it is reported rather than
	// being an assertion that aborts.
	AccountingErrors int
}

// simWorker is the simulator's own view of a worker, tracking the failure
// state it injects. The control plane's view lives in its registry.
type simWorker struct {
	id      string
	stalled bool
	removed bool
}

type simTask struct {
	id          string
	tenant      string
	submittedAt time.Duration
	placedAt    time.Duration
	endsAt      time.Duration
	duration    time.Duration
	placed      bool
	running     bool
	done        bool
}

// Run executes one scenario.
func Run(sc Scenario) (Result, error) {
	rng := rand.New(rand.NewSource(sc.Seed))
	cfg := sc.Config
	if cfg.HeartbeatTimeout == 0 {
		cfg = dovetail.DefaultConfig()
	}

	sched, err := dovetail.NewScheduler(cfg)
	if err != nil {
		return Result{}, err
	}

	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	now := func(elapsed time.Duration) time.Time { return base.Add(elapsed) }

	zones := sc.Zones
	if len(zones) == 0 {
		zones = []string{"a"}
	}

	// Fleet.
	workers := make([]*simWorker, 0, sc.Workers)
	for i := 0; i < sc.Workers; i++ {
		id := fmt.Sprintf("w%02d", i)
		zone := zones[i%len(zones)]
		if _, err := sched.Registry().Register(id, sc.WorkerCapacity,
			dovetail.Topology{Zone: zone}, now(0)); err != nil {
			return Result{}, err
		}
		// One heartbeat so the worker is placeable rather than warming.
		if err := sched.Registry().Heartbeat(id, sc.WorkerCapacity, nil, now(0)); err != nil {
			return Result{}, err
		}
		workers = append(workers, &simWorker{id: id})
	}

	for _, tp := range sc.Tenants {
		if err := sched.AddTenant(dovetail.Tenant{ID: tp.ID, Quota: tp.Quota, Weight: tp.Weight}); err != nil {
			return Result{}, err
		}
	}

	result := Result{
		Scenario:           sc.Name,
		PerTenantCompleted: map[string]int{},
		PerTenantQueueP95:  map[string]time.Duration{},
	}
	tasks := map[string]*simTask{}
	var utilCPU, utilMem, fairness []float64
	jobSeq := 0

	for elapsed := time.Duration(0); elapsed < sc.Duration; elapsed += sc.CycleInterval {
		clock := now(elapsed)

		// Injected failures.
		if sc.KillWorkerAt > 0 && elapsed >= sc.KillWorkerAt && !workers[0].removed {
			sched.Registry().Remove(workers[0].id)
			workers[0].removed = true
		}
		if sc.StallWorkerAt > 0 && elapsed >= sc.StallWorkerAt && len(workers) > 1 {
			// Stop heartbeating. The control plane must notice on its own,
			// which is the realistic failure and the one with a latency cost.
			workers[1].stalled = true
		}

		// Submissions.
		for _, tp := range sc.Tenants {
			expected := tp.SubmitRate * sc.CycleInterval.Seconds()
			n := int(expected)
			if rng.Float64() < expected-float64(n) {
				n++
			}
			for k := 0; k < n; k++ {
				jobSeq++
				count := tp.TasksPerJobMin
				if tp.TasksPerJobMax > tp.TasksPerJobMin {
					count += rng.Intn(tp.TasksPerJobMax - tp.TasksPerJobMin + 1)
				}
				if count < 1 {
					count = 1
				}
				job := &dovetail.Job{
					ID:     fmt.Sprintf("%s-j%d", tp.ID, jobSeq),
					Tenant: tp.ID,
					Gang:   rng.Float64() < tp.GangFraction,
				}
				for i := 0; i < count; i++ {
					id := fmt.Sprintf("%s-t%d", job.ID, i)
					job.Tasks = append(job.Tasks, &dovetail.Task{ID: id, Request: tp.Request})
					d := time.Duration(rng.ExpFloat64() * float64(tp.DurationMean))
					if d < sc.CycleInterval {
						d = sc.CycleInterval
					}
					tasks[id] = &simTask{id: id, tenant: tp.ID, submittedAt: elapsed, duration: d}
				}
				if err := sched.Submit(job, clock); err != nil {
					// A rejected submission is a real outcome (task larger
					// than any worker, unknown tenant) and is counted rather
					// than aborting the run.
					for _, t := range job.Tasks {
						delete(tasks, t.ID)
					}
					continue
				}
				result.TasksSubmitted += count
			}
		}

		// Scheduling cycle.
		cycle, err := sched.Cycle(clock)
		if err != nil {
			result.AccountingErrors++
		}

		// Workers pick up their assignments and report.
		for _, d := range cycle.Decisions {
			st := tasks[d.TaskID]
			if st == nil {
				continue
			}
			if !st.placed {
				st.placed = true
				st.placedAt = elapsed
			}
			w := findWorker(workers, d.WorkerID)
			if w == nil || w.stalled || w.removed {
				// The command is lost. The reservation will lapse, which is
				// exactly the path the reservation timeout exists for.
				continue
			}
			if err := sched.TaskStarted(d.TaskID, d.WorkerID, clock); err == nil {
				st.running = true
				st.endsAt = elapsed + st.duration
			}
		}

		// Completions.
		for _, st := range tasks {
			if st.running && !st.done && elapsed >= st.endsAt {
				if err := sched.TaskFinished(st.id, true, "", clock); err == nil {
					st.done = true
					st.running = false
					result.TasksCompleted++
					result.PerTenantCompleted[st.tenant]++
				}
			}
		}

		// Heartbeats. A stalled worker simply does not send one.
		for _, w := range workers {
			if w.stalled || w.removed {
				continue
			}
			running, used := runningOn(sched, w.id)
			free := sc.WorkerCapacity.Sub(used).Clamp()
			_ = sched.Registry().Heartbeat(w.id, free, running, clock)
		}

		// Periodic reconciliation, at a tenth the cycle rate.
		if int(elapsed/sc.CycleInterval)%10 == 0 {
			sched.Reconcile(clock)
			if err := sched.AuditAccounting(); err != nil {
				result.AccountingErrors++
			}
		}

		cpu, mem := sched.Utilization(clock)
		utilCPU = append(utilCPU, cpu)
		utilMem = append(utilMem, mem)
		if cpu > result.PeakCPUUtil {
			result.PeakCPUUtil = cpu
		}
		fairness = append(fairness, dovetail.FairnessIndex(sched.Shares(clock)))
	}

	// Latencies.
	var all []time.Duration
	perTenant := map[string][]time.Duration{}
	for _, st := range tasks {
		if !st.placed {
			continue
		}
		lat := st.placedAt - st.submittedAt
		all = append(all, lat)
		perTenant[st.tenant] = append(perTenant[st.tenant], lat)
	}
	result.QueueLatencyP50 = percentile(all, 0.50)
	result.QueueLatencyP95 = percentile(all, 0.95)
	result.QueueLatencyMax = percentile(all, 1.0)
	for tenant, lats := range perTenant {
		result.PerTenantQueueP95[tenant] = percentile(lats, 0.95)
	}

	result.MeanCPUUtil = mean(utilCPU)
	result.MeanMemUtil = mean(utilMem)
	result.MeanFairness = mean(fairness)
	result.Stats = sched.Stats()

	result.TasksFailed = result.TasksSubmitted - result.TasksCompleted

	return result, nil
}

func findWorker(workers []*simWorker, id string) *simWorker {
	for _, w := range workers {
		if w.id == id {
			return w
		}
	}
	return nil
}

// runningOn returns the tasks a worker is running per the control plane, and
// their total resources.
//
// The simulator uses the control plane's own view to generate the worker's
// heartbeat, which is a deliberate simplification and a real limitation: it
// means the worker can never disagree with the control plane except through
// the injected failures above. Genuine divergence — a worker running something
// the control plane never asked for — is covered by the unit tests rather than
// here, and the orphan-adoption path is therefore under-exercised by the
// simulator. Stated in bench/results.md.
func runningOn(sched *dovetail.Scheduler, workerID string) ([]string, dovetail.Resources) {
	var ids []string
	var used dovetail.Resources
	for _, a := range sched.AssignmentsOn(workerID) {
		if a.Confirmed {
			ids = append(ids, a.TaskID)
			used = used.Add(a.Request)
		}
	}
	sort.Strings(ids)
	return ids, used
}

func percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), d...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func mean(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	var sum float64
	for _, x := range v {
		sum += x
	}
	return sum / float64(len(v))
}

// Report renders a result as aligned text.
func (r Result) Report() string {
	out := fmt.Sprintf("\n=== %s ===\n", r.Scenario)
	out += fmt.Sprintf("  tasks            %d submitted, %d completed\n", r.TasksSubmitted, r.TasksCompleted)
	out += fmt.Sprintf("  queue latency    p50 %s  p95 %s  max %s\n",
		r.QueueLatencyP50.Round(time.Millisecond),
		r.QueueLatencyP95.Round(time.Millisecond),
		r.QueueLatencyMax.Round(time.Millisecond))
	out += fmt.Sprintf("  utilization      cpu %.1f%% (peak %.1f%%)  mem %.1f%%\n",
		r.MeanCPUUtil*100, r.PeakCPUUtil*100, r.MeanMemUtil*100)
	out += fmt.Sprintf("  fairness index   %.3f\n", r.MeanFairness)
	out += fmt.Sprintf("  per tenant       ")
	for _, k := range sortedStringKeys(r.PerTenantCompleted) {
		out += fmt.Sprintf("%s=%d(p95 %s) ", k, r.PerTenantCompleted[k],
			r.PerTenantQueueP95[k].Round(time.Millisecond))
	}
	out += "\n"
	out += fmt.Sprintf("  scheduler        %d cycles, %d placed, %d quota rejections, %d gangs deferred\n",
		r.Stats.Cycles, r.Stats.Placed, r.Stats.QuotaRejections, r.Stats.GangsDeferred)
	out += fmt.Sprintf("  failures         %d workers lost, %d tasks lost, %d retried, %d reservations lapsed\n",
		r.Stats.WorkersLost, r.Stats.TasksLost, r.Stats.TasksRetried, r.Stats.ReservationsLapsed)
	if r.AccountingErrors > 0 {
		out += fmt.Sprintf("  ** %d ACCOUNTING ERRORS — this run is invalid **\n", r.AccountingErrors)
	}
	return out
}

func sortedStringKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
