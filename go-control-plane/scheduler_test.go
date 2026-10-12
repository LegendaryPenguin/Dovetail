package dovetail

import (
	"fmt"
	"testing"
	"time"
)

var t0 = time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

// harness builds a scheduler with a fleet and tenants, driven by an explicit
// clock.
//
// Every test here passes time in rather than reading the wall clock. A
// scheduler is a timing-sensitive system — heartbeat timeouts, reservation
// expiry, staleness — and tests that sleep to advance time are both slow and
// flaky. Nothing in this package calls time.Now(), which is what makes the
// expiry tests deterministic.
type harness struct {
	t   *testing.T
	s   *Scheduler
	now time.Time
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	s, err := NewScheduler(cfg)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	return &harness{t: t, s: s, now: t0}
}

func (h *harness) advance(d time.Duration) { h.now = h.now.Add(d) }

func (h *harness) addWorker(id string, cap Resources, zone string) {
	h.t.Helper()
	if _, err := h.s.registry.Register(id, cap, Topology{Zone: zone}, h.now); err != nil {
		h.t.Fatalf("register %s: %v", id, err)
	}
	// Registration leaves a worker warming. One heartbeat makes it placeable,
	// which is the normal startup sequence.
	h.beat(id, cap, nil)
}

func (h *harness) beat(id string, free Resources, running []string) {
	h.t.Helper()
	if err := h.s.registry.Heartbeat(id, free, running, h.now); err != nil {
		h.t.Fatalf("heartbeat %s: %v", id, err)
	}
}

func (h *harness) addTenant(id string, quota Resources, weight float64) {
	h.t.Helper()
	if err := h.s.AddTenant(Tenant{ID: id, Quota: quota, Weight: weight}); err != nil {
		h.t.Fatalf("add tenant %s: %v", id, err)
	}
}

func (h *harness) submit(jobID, tenant string, n int, req Resources, gang bool) *Job {
	h.t.Helper()
	j := &Job{ID: jobID, Tenant: tenant, Gang: gang}
	for i := 0; i < n; i++ {
		j.Tasks = append(j.Tasks, &Task{ID: fmt.Sprintf("%s-t%d", jobID, i), Request: req})
	}
	if err := h.s.Submit(j, h.now); err != nil {
		h.t.Fatalf("submit %s: %v", jobID, err)
	}
	return j
}

func (h *harness) cycle() CycleResult {
	h.t.Helper()
	r, err := h.s.Cycle(h.now)
	if err != nil {
		h.t.Fatalf("cycle: %v", err)
	}
	if err := h.s.AuditAccounting(); err != nil {
		h.t.Fatalf("accounting audit after cycle %d: %v", r.Epoch, err)
	}
	return r
}

// confirmAll simulates every assigned task being picked up by its worker,
// including the worker's free-capacity report dropping accordingly.
func (h *harness) confirmAll() {
	h.t.Helper()
	perWorker := map[string][]string{}
	used := map[string]Resources{}

	h.s.mu.Lock()
	for id, t := range h.s.tasks {
		if t.Phase == PhaseAssigned || t.Phase == PhaseRunning {
			perWorker[t.WorkerID] = append(perWorker[t.WorkerID], id)
			used[t.WorkerID] = used[t.WorkerID].Add(t.Request)
		}
	}
	h.s.mu.Unlock()

	for _, w := range h.s.registry.Snapshot() {
		for _, id := range perWorker[w.ID] {
			if err := h.s.TaskStarted(id, w.ID, h.now); err != nil {
				// Already running is fine; this helper is called repeatedly.
				continue
			}
		}
		h.beat(w.ID, w.Capacity.Sub(used[w.ID]), perWorker[w.ID])
	}
}

func (h *harness) phaseCount() map[Phase]int {
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	out := map[Phase]int{}
	for _, t := range h.s.tasks {
		out[t.Phase]++
	}
	return out
}

// ---------------------------------------------------------------------------
// The bug this project is shaped around
// ---------------------------------------------------------------------------

func TestNoDoubleBookingWithinOneCycle(t *testing.T) {
	// The original bug: the scheduler read the worker's reported free
	// capacity, which predated its own decisions, and placed two tasks into
	// the same gap. One worker with room for exactly two tasks must receive
	// exactly two, not four, in a single cycle — before any heartbeat has had
	// a chance to report the first.
	h := newHarness(t, DefaultConfig())
	h.addWorker("w1", Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("j1", "acme", 6, Resources{MilliCPU: CPU(2), MemBytes: GiB(4)}, false)

	r := h.cycle()

	if len(r.Decisions) != 2 {
		t.Fatalf("placed %d tasks on a worker with room for 2", len(r.Decisions))
	}
	// And the ledger must agree that the worker is now full.
	w, _ := h.s.registry.Get("w1")
	h.s.mu.Lock()
	avail := h.s.ledger.Available(w)
	h.s.mu.Unlock()
	if avail.MilliCPU != 0 || avail.MemBytes != 0 {
		t.Fatalf("worker has %s available after being filled", avail)
	}
}

func TestConfirmedAssignmentsAreNotDoubleCounted(t *testing.T) {
	// The mirror-image bug. Once a heartbeat reports a task running, the
	// worker's own free figure accounts for it, so continuing to subtract our
	// reservation would make the worker look full at half utilisation.
	h := newHarness(t, DefaultConfig())
	cap := Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}
	h.addWorker("w1", cap, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("j1", "acme", 1, Resources{MilliCPU: CPU(2), MemBytes: GiB(4)}, false)

	h.cycle()
	h.confirmAll()

	// The worker now reports 2 CPU / 4 GiB free and we hold one confirmed
	// reservation. Available must be 2 CPU, not 0.
	w, _ := h.s.registry.Get("w1")
	h.s.mu.Lock()
	avail := h.s.ledger.Available(w)
	unconfirmed := h.s.ledger.Unconfirmed("w1")
	h.s.mu.Unlock()

	if !unconfirmed.IsZero() {
		t.Errorf("unconfirmed should be zero after confirmation, got %s", unconfirmed)
	}
	if avail.MilliCPU != CPU(2) || avail.MemBytes != GiB(4) {
		t.Fatalf("available after confirmation is %s, want {cpu=2000m mem=4GiB}", avail)
	}

	// So a second task of the same size must now fit.
	h.submit("j2", "acme", 1, Resources{MilliCPU: CPU(2), MemBytes: GiB(4)}, false)
	r := h.cycle()
	if len(r.Decisions) != 1 {
		t.Fatalf("a second task did not fit the remaining half of the worker: %+v", r.Unplaced)
	}
}

func TestAvailableNeverGoesNegativeAcrossManyCycles(t *testing.T) {
	// The invariant the whole ledger exists to maintain, driven hard. The
	// Cycle method itself asserts this, so a failure surfaces as a cycle
	// error; this test is here to generate the traffic that would trip it.
	h := newHarness(t, DefaultConfig())
	for i := 0; i < 5; i++ {
		h.addWorker(fmt.Sprintf("w%d", i), Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}, "a")
	}
	h.addTenant("acme", Resources{MilliCPU: CPU(1000), MemBytes: GiB(2000)}, 1)

	for round := 0; round < 20; round++ {
		h.submit(fmt.Sprintf("j%d", round), "acme", 7,
			Resources{MilliCPU: CPU(1), MemBytes: GiB(2)}, false)
		h.cycle()
		h.confirmAll()
		h.advance(time.Second)

		// Finish roughly half the running work to keep capacity churning.
		h.s.mu.Lock()
		var running []string
		for id, task := range h.s.tasks {
			if task.Phase == PhaseRunning {
				running = append(running, id)
			}
		}
		h.s.mu.Unlock()
		for i, id := range running {
			if i%2 == 0 {
				if err := h.s.TaskFinished(id, true, "", h.now); err != nil {
					t.Fatalf("finish %s: %v", id, err)
				}
			}
		}
		h.confirmAll()
	}
}

// ---------------------------------------------------------------------------
// Quota and fairness
// ---------------------------------------------------------------------------

func TestQuotaIsAHardCeilingEvenOnAnIdleCluster(t *testing.T) {
	// The distinction between a quota and a share. An idle cluster does not
	// let a tenant past its quota — that is what makes capacity planning
	// possible.
	h := newHarness(t, DefaultConfig())
	h.addWorker("w1", Resources{MilliCPU: CPU(64), MemBytes: GiB(128)}, "a")
	h.addTenant("small", Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, 1)
	h.submit("j1", "small", 10, Resources{MilliCPU: CPU(2), MemBytes: GiB(4)}, false)

	r := h.cycle()
	if len(r.Decisions) != 2 {
		t.Fatalf("placed %d tasks for a tenant whose quota allows 2", len(r.Decisions))
	}
	found := false
	for _, reason := range r.Unplaced {
		if reason != "" && len(reason) > 0 && reason[:6] == "tenant" {
			found = true
		}
	}
	if !found {
		t.Errorf("no task was rejected for quota; reasons: %+v", r.Unplaced)
	}
}

func TestOneTenantCannotStarveAnother(t *testing.T) {
	// The failure that motivated fair share. A tenant submitting a hundred
	// tasks must not lock out a tenant submitting two, given both have quota
	// headroom.
	h := newHarness(t, DefaultConfig())
	for i := 0; i < 4; i++ {
		h.addWorker(fmt.Sprintf("w%d", i), Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}, "a")
	}
	big := Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}
	h.addTenant("greedy", big, 1)
	h.addTenant("modest", big, 1)

	h.submit("greedy-job", "greedy", 100, Resources{MilliCPU: CPU(1), MemBytes: GiB(2)}, false)
	h.submit("modest-job", "modest", 2, Resources{MilliCPU: CPU(1), MemBytes: GiB(2)}, false)

	h.cycle()

	placedFor := map[string]int{}
	h.s.mu.Lock()
	for _, task := range h.s.tasks {
		if task.Phase.Occupies() {
			placedFor[task.Tenant]++
		}
	}
	h.s.mu.Unlock()

	if placedFor["modest"] == 0 {
		t.Fatalf("the modest tenant got nothing; greedy got %d", placedFor["greedy"])
	}
}

func TestFairShareConvergesWithWeights(t *testing.T) {
	// Weight 3 against weight 1 converges toward 75/25 of *contested*
	// capacity. The contention is the point: with demand below capacity both
	// tenants simply fit and the weights never bind, which is how the first
	// version of this test passed while proving nothing. 8 workers x 4 CPU is
	// 32 CPU of capacity against 80 CPU of demand.
	//
	// DRF equilibrium: shares equalise when heavy/3 == light/1 and the two sum
	// to the full cluster, so heavy = 0.75 and light = 0.25.
	h := newHarness(t, DefaultConfig())
	for i := 0; i < 8; i++ {
		h.addWorker(fmt.Sprintf("w%d", i), Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, "a")
	}
	big := Resources{MilliCPU: CPU(1000), MemBytes: GiB(2000)}
	h.addTenant("heavy", big, 3)
	h.addTenant("light", big, 1)

	// Oversubscribe both tenants and never finish anything, so every cycle is
	// a pure contention decision.
	h.submit("heavy-job", "heavy", 40, Resources{MilliCPU: CPU(1), MemBytes: GiB(2)}, false)
	h.submit("light-job", "light", 40, Resources{MilliCPU: CPU(1), MemBytes: GiB(2)}, false)

	for round := 0; round < 6; round++ {
		h.cycle()
		h.confirmAll()
		h.advance(time.Second)
	}

	usage := map[string]Resources{}
	h.s.mu.Lock()
	for _, task := range h.s.tasks {
		if task.Phase.Occupies() {
			usage[task.Tenant] = usage[task.Tenant].Add(task.Request)
		}
	}
	h.s.mu.Unlock()

	heavy := float64(usage["heavy"].MilliCPU)
	light := float64(usage["light"].MilliCPU)
	if heavy+light == 0 {
		t.Fatal("nothing was placed")
	}
	// The cluster must actually be full, or there was no contention to resolve.
	if heavy+light < float64(CPU(30)) {
		t.Fatalf("cluster is not contended: only %.0fm CPU of 32000m placed", heavy+light)
	}
	ratio := heavy / (heavy + light)
	// Loose bound: integral task sizes mean the ratio lands near but not on
	// 0.75, and asserting tighter would fail for reasons that are not bugs.
	if ratio < 0.65 || ratio > 0.85 {
		t.Errorf("heavy tenant holds %.2f of CPU, want roughly 0.75 (heavy=%v light=%v)",
			ratio, usage["heavy"], usage["light"])
	}
}

func TestUnknownTenantIsRefusedNotGivenInfiniteQuota(t *testing.T) {
	h := newHarness(t, DefaultConfig())
	h.addWorker("w1", Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}, "a")
	j := &Job{ID: "j1", Tenant: "nonexistent", Tasks: []*Task{
		{ID: "t1", Request: Resources{MilliCPU: CPU(1), MemBytes: GiB(1)}},
	}}
	if err := h.s.Submit(j, h.now); err == nil {
		t.Fatal("a job for an unknown tenant was accepted")
	}
}

func TestDominantResourceShareUsesTheRightDimension(t *testing.T) {
	total := Resources{MilliCPU: CPU(100), MemBytes: GiB(100)}
	// 60% of CPU, 10% of memory. The dominant share is 0.6 via CPU.
	r := Resources{MilliCPU: CPU(60), MemBytes: GiB(10)}
	share, dim := r.DominantShare(total)
	if dim != "cpu" {
		t.Errorf("dominant dimension = %q, want cpu", dim)
	}
	if share < 0.59 || share > 0.61 {
		t.Errorf("dominant share = %.3f, want 0.60", share)
	}

	// And the memory-heavy mirror image.
	r = Resources{MilliCPU: CPU(10), MemBytes: GiB(80)}
	share, dim = r.DominantShare(total)
	if dim != "mem" || share < 0.79 || share > 0.81 {
		t.Errorf("got %.3f via %s, want 0.80 via mem", share, dim)
	}
}

// ---------------------------------------------------------------------------
// Failure handling
// ---------------------------------------------------------------------------

func TestStaleWorkerReturnsItsTasksToTheQueue(t *testing.T) {
	cfg := DefaultConfig()
	h := newHarness(t, cfg)
	h.addWorker("w1", Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, "a")
	h.addWorker("w2", Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("j1", "acme", 2, Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, false)

	h.cycle()
	h.confirmAll()
	if c := h.phaseCount(); c[PhaseRunning] != 2 {
		t.Fatalf("expected 2 running, got %+v", c)
	}

	// w1 goes quiet; w2 keeps beating.
	h.advance(cfg.HeartbeatTimeout + time.Second)
	h.beat("w2", Resources{MilliCPU: 0, MemBytes: 0}, []string{"j1-t1"})

	r := h.cycle()
	if len(r.Lost) != 1 || r.Lost[0] != "w1" {
		t.Fatalf("expected w1 to be reaped, got %+v", r.Lost)
	}
	// Its task is requeued, not failed: it may still be running out there.
	st, _ := h.s.Task("j1-t0")
	if st.Phase != PhasePending {
		t.Errorf("task on the lost worker is %s, want pending", st.Phase)
	}
	if h.s.Stats().TasksRetried == 0 {
		t.Error("no retry was counted")
	}
}

func TestWarmingWorkerIsNotReapedAndNotPlacedOn(t *testing.T) {
	// The bug that made cold starts look like crashes. A worker that has
	// registered but not yet heartbeated is neither healthy nor stale.
	cfg := DefaultConfig()
	h := newHarness(t, cfg)
	if _, err := h.s.registry.Register("cold", Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}, Topology{Zone: "a"}, h.now); err != nil {
		t.Fatalf("register: %v", err)
	}
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("j1", "acme", 1, Resources{MilliCPU: CPU(1), MemBytes: GiB(1)}, false)

	w, _ := h.s.registry.Get("cold")
	if !w.Warming() {
		t.Fatal("a freshly registered worker should be warming")
	}
	if w.Stale(h.now, cfg.HeartbeatTimeout) {
		t.Fatal("a freshly registered worker must not be stale")
	}

	// Nothing places on it, because it has never reported.
	r := h.cycle()
	if len(r.Decisions) != 0 {
		t.Fatalf("placed work on a worker that has never heartbeated: %+v", r.Decisions)
	}
	if len(r.Lost) != 0 {
		t.Fatalf("reaped a warming worker: %+v", r.Lost)
	}

	// Once it reports, it becomes placeable.
	h.beat("cold", Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}, nil)
	r = h.cycle()
	if len(r.Decisions) != 1 {
		t.Fatalf("a warm worker did not receive work: %+v", r.Unplaced)
	}
}

func TestUnconfirmedReservationExpiresIndependentlyOfHeartbeats(t *testing.T) {
	// A worker can be alive and heartbeating and still have dropped one
	// assignment command. The reservation timeout must reclaim that capacity
	// without waiting for the heartbeat timeout.
	cfg := DefaultConfig()
	cfg.ReservationTimeout = 2 * time.Second
	cfg.HeartbeatTimeout = 30 * time.Second
	h := newHarness(t, cfg)
	h.addWorker("w1", Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("j1", "acme", 1, Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, false)

	h.cycle()
	h.s.mu.Lock()
	held := h.s.ledger.Unconfirmed("w1")
	h.s.mu.Unlock()
	if held.IsZero() {
		t.Fatal("no reservation was held after placement")
	}

	// The worker keeps heartbeating but never reports the task.
	h.advance(cfg.ReservationTimeout + time.Second)
	h.beat("w1", Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, nil)

	r := h.cycle()
	if len(r.Expired) != 1 {
		t.Fatalf("expected one lapsed reservation, got %d", len(r.Expired))
	}
	if len(r.Lost) != 0 {
		t.Fatalf("a healthy worker was reaped: %+v", r.Lost)
	}
	if h.s.Stats().ReservationsLapsed != 1 {
		t.Error("lapsed reservation was not counted")
	}
}

func TestConfigRejectsReservationTimeoutLongerThanHeartbeat(t *testing.T) {
	// If a reservation outlives the heartbeat window, the staleness path
	// always wins and the reservation expiry becomes dead code that still
	// looks live.
	_, err := NewScheduler(Config{HeartbeatTimeout: 5 * time.Second, ReservationTimeout: 10 * time.Second})
	if err == nil {
		t.Fatal("accepted a reservation timeout longer than the heartbeat timeout")
	}
}

func TestAttemptsAreBoundedSoATaskCannotLoopForever(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxAttempts = 2
	h := newHarness(t, cfg)
	h.addWorker("w1", Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("j1", "acme", 1, Resources{MilliCPU: CPU(1), MemBytes: GiB(1)}, false)

	for i := 0; i < 5; i++ {
		h.cycle()
		h.s.mu.Lock()
		task := h.s.tasks["j1-t0"]
		phase := task.Phase
		h.s.mu.Unlock()
		if phase == PhaseFailed {
			break
		}
		if err := h.s.TaskFinished("j1-t0", false, "boom", h.now); err != nil {
			t.Fatalf("finish: %v", err)
		}
		h.advance(time.Second)
	}

	st, _ := h.s.Task("j1-t0")
	if st.Phase != PhaseFailed {
		t.Fatalf("task is %s after repeated failures, want failed", st.Phase)
	}
	if st.Attempts > cfg.MaxAttempts {
		t.Errorf("task ran %d attempts, max is %d", st.Attempts, cfg.MaxAttempts)
	}
}

func TestDrainingWorkerKeepsWorkButTakesNoMore(t *testing.T) {
	h := newHarness(t, DefaultConfig())
	h.addWorker("w1", Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("j1", "acme", 1, Resources{MilliCPU: CPU(1), MemBytes: GiB(1)}, false)
	h.cycle()
	h.confirmAll()

	if err := h.s.registry.SetDraining("w1", true); err != nil {
		t.Fatalf("drain: %v", err)
	}
	h.submit("j2", "acme", 1, Resources{MilliCPU: CPU(1), MemBytes: GiB(1)}, false)
	r := h.cycle()

	if len(r.Decisions) != 0 {
		t.Fatalf("placed work on a draining worker: %+v", r.Decisions)
	}
	if reason := r.Unplaced["j2-t0"]; reason != ReasonDraining {
		t.Errorf("unplaced reason = %q, want %q", reason, ReasonDraining)
	}
	// The existing task is untouched.
	st, _ := h.s.Task("j1-t0")
	if st.Phase != PhaseRunning {
		t.Errorf("draining disturbed a running task: %s", st.Phase)
	}
}

// ---------------------------------------------------------------------------
// Reconciliation
// ---------------------------------------------------------------------------

func TestReconcileReleasesGhosts(t *testing.T) {
	// The control plane thinks a confirmed task is running; the worker stops
	// reporting it. The worker is authoritative about its own processes.
	h := newHarness(t, DefaultConfig())
	cap := Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}
	h.addWorker("w1", cap, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("j1", "acme", 1, Resources{MilliCPU: CPU(2), MemBytes: GiB(4)}, false)
	h.cycle()
	h.confirmAll()

	// Worker now reports nothing running, with all capacity free.
	h.advance(time.Second)
	h.beat("w1", cap, nil)

	res := h.s.Reconcile(h.now)
	if res.Released != 1 {
		t.Fatalf("released %d ghosts, want 1", res.Released)
	}
	if len(res.Discrepancies) != 1 || res.Discrepancies[0].Kind != DiscrepancyGhost {
		t.Fatalf("discrepancies = %+v", res.Discrepancies)
	}
	st, _ := h.s.Task("j1-t0")
	if st.Phase != PhasePending {
		t.Errorf("ghost task is %s, want pending", st.Phase)
	}
	if err := h.s.AuditAccounting(); err != nil {
		t.Errorf("accounting broken after reconcile: %v", err)
	}
}

func TestReconcileDoesNotTreatUnconfirmedReservationsAsGhosts(t *testing.T) {
	// A reservation inside its grace period is not a ghost — the worker may
	// simply not have started it yet. Treating it as one would race the
	// reservation timeout and release capacity a moment before the worker
	// reports the task running.
	h := newHarness(t, DefaultConfig())
	cap := Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}
	h.addWorker("w1", cap, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("j1", "acme", 1, Resources{MilliCPU: CPU(2), MemBytes: GiB(4)}, false)
	h.cycle()

	// Worker heartbeats without the task: it has not started it yet.
	h.beat("w1", cap, nil)

	res := h.s.Reconcile(h.now)
	if res.Released != 0 {
		t.Fatalf("released an unconfirmed reservation as a ghost: %+v", res.Discrepancies)
	}
	st, _ := h.s.Task("j1-t0")
	if st.Phase != PhaseAssigned {
		t.Errorf("task is %s, want assigned", st.Phase)
	}
}

func TestReconcileAdoptsKnownOrphans(t *testing.T) {
	// The worker is running a task we know about but have lost track of. The
	// work is real and consuming real capacity, so adopt it.
	h := newHarness(t, DefaultConfig())
	cap := Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}
	h.addWorker("w1", cap, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("j1", "acme", 1, Resources{MilliCPU: CPU(2), MemBytes: GiB(4)}, false)
	h.cycle()
	h.confirmAll()

	// Simulate the control plane losing its ledger entry while the worker
	// keeps running the task.
	h.s.mu.Lock()
	h.s.ledger.Release("j1-t0")
	h.s.tasks["j1-t0"].Phase = PhasePending
	h.s.tasks["j1-t0"].WorkerID = ""
	h.s.mu.Unlock()

	h.beat("w1", cap.Sub(Resources{MilliCPU: CPU(2), MemBytes: GiB(4)}), []string{"j1-t0"})
	res := h.s.Reconcile(h.now)

	if res.Adopted != 1 {
		t.Fatalf("adopted %d orphans, want 1: %+v", res.Adopted, res.Discrepancies)
	}
	st, _ := h.s.Task("j1-t0")
	if st.Phase != PhaseRunning || st.WorkerID != "w1" {
		t.Errorf("adopted task is %s on %q", st.Phase, st.WorkerID)
	}
	if err := h.s.AuditAccounting(); err != nil {
		t.Errorf("accounting broken after adoption: %v", err)
	}
}

func TestReconcileReportsUnknownOrphansWithoutAdopting(t *testing.T) {
	// A task from a previous control-plane lifetime. We cannot adopt it
	// without knowing its resource request, and inventing one would corrupt
	// the accounting reconciliation exists to protect.
	h := newHarness(t, DefaultConfig())
	cap := Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}
	h.addWorker("w1", cap, "a")
	h.beat("w1", cap, []string{"task-from-a-past-life"})

	res := h.s.Reconcile(h.now)
	if res.Adopted != 0 {
		t.Fatal("adopted a task with no known resource request")
	}
	if len(res.Discrepancies) != 1 || res.Discrepancies[0].Kind != DiscrepancyOrphan {
		t.Fatalf("discrepancies = %+v", res.Discrepancies)
	}
}

func TestReconcileSkipsStaleWorkers(t *testing.T) {
	// Reconciling against a worker that has not spoken would read its silence
	// as "running nothing" and release every task on a machine that is merely
	// slow to heartbeat.
	cfg := DefaultConfig()
	h := newHarness(t, cfg)
	cap := Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}
	h.addWorker("w1", cap, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("j1", "acme", 1, Resources{MilliCPU: CPU(2), MemBytes: GiB(4)}, false)
	h.cycle()
	h.confirmAll()

	h.advance(cfg.HeartbeatTimeout + time.Second)
	res := h.s.Reconcile(h.now)
	if res.Released != 0 {
		t.Fatalf("reconciliation released tasks on a merely-silent worker: %+v", res.Discrepancies)
	}
}

// ---------------------------------------------------------------------------
// Gang scheduling
// ---------------------------------------------------------------------------

func TestGangIsAllOrNothing(t *testing.T) {
	// Room for three of four tasks. Nothing may be placed — a half-placed
	// gang holds capacity and makes no progress.
	h := newHarness(t, DefaultConfig())
	h.addWorker("w1", Resources{MilliCPU: CPU(3), MemBytes: GiB(6)}, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("g1", "acme", 4, Resources{MilliCPU: CPU(1), MemBytes: GiB(2)}, true)

	r := h.cycle()
	if len(r.Decisions) != 0 {
		t.Fatalf("placed %d of a 4-task gang into room for 3", len(r.Decisions))
	}
	if r.Unplaced["g1-t0"] != ReasonGangInfeasible {
		t.Errorf("reason = %q, want %q", r.Unplaced["g1-t0"], ReasonGangInfeasible)
	}
	if h.s.Stats().GangsDeferred != 1 {
		t.Error("deferred gang was not counted")
	}
	// No capacity was consumed by the failed attempt.
	w, _ := h.s.registry.Get("w1")
	h.s.mu.Lock()
	avail := h.s.ledger.Available(w)
	h.s.mu.Unlock()
	if avail != w.Capacity {
		t.Errorf("a failed gang attempt consumed capacity: %s of %s free", avail, w.Capacity)
	}
}

func TestGangPlacesWhenItFits(t *testing.T) {
	h := newHarness(t, DefaultConfig())
	h.addWorker("w1", Resources{MilliCPU: CPU(2), MemBytes: GiB(4)}, "a")
	h.addWorker("w2", Resources{MilliCPU: CPU(2), MemBytes: GiB(4)}, "b")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("g1", "acme", 4, Resources{MilliCPU: CPU(1), MemBytes: GiB(2)}, true)

	r := h.cycle()
	if len(r.Decisions) != 4 {
		t.Fatalf("placed %d of 4 gang tasks that fit: %+v", len(r.Decisions), r.Unplaced)
	}
}

func TestGangQuotaIsCheckedAgainstTheWholeSet(t *testing.T) {
	// Admitting half a gang because half fits the quota headroom reproduces
	// the partial-placement deadlock through a different door.
	h := newHarness(t, DefaultConfig())
	h.addWorker("w1", Resources{MilliCPU: CPU(64), MemBytes: GiB(128)}, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(3), MemBytes: GiB(6)}, 1)
	h.submit("g1", "acme", 4, Resources{MilliCPU: CPU(1), MemBytes: GiB(2)}, true)

	r := h.cycle()
	if len(r.Decisions) != 0 {
		t.Fatalf("placed %d gang tasks exceeding the tenant quota", len(r.Decisions))
	}
}

func TestGangPacksLargestFirst(t *testing.T) {
	// A gang of mixed sizes placed smallest-first fragments the cluster and
	// then cannot fit its own largest member, even though a working ordering
	// existed.
	h := newHarness(t, DefaultConfig())
	h.addWorker("w1", Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, "a")
	h.addWorker("w2", Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)

	j := &Job{ID: "g1", Tenant: "acme", Gang: true, Tasks: []*Task{
		{ID: "g1-small-a", Request: Resources{MilliCPU: CPU(1), MemBytes: GiB(2)}},
		{ID: "g1-small-b", Request: Resources{MilliCPU: CPU(1), MemBytes: GiB(2)}},
		{ID: "g1-big", Request: Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}},
	}}
	if err := h.s.Submit(j, h.now); err != nil {
		t.Fatalf("submit: %v", err)
	}

	r := h.cycle()
	if len(r.Decisions) != 3 {
		t.Fatalf("placed %d of 3: largest-first ordering failed: %+v", len(r.Decisions), r.Unplaced)
	}
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

func TestCyclesAreDeterministic(t *testing.T) {
	// Without this, two simulator runs on identical input diverge, every
	// policy comparison becomes noise, and bug reproductions stop
	// reproducing. Go randomises map iteration, so determinism has to be
	// built rather than assumed.
	run := func() []Decision {
		h := newHarness(t, DefaultConfig())
		for i := 0; i < 6; i++ {
			h.addWorker(fmt.Sprintf("w%d", i), Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, fmt.Sprintf("z%d", i%3))
		}
		big := Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}
		for _, tn := range []string{"a", "b", "c"} {
			h.addTenant(tn, big, 1)
			h.submit("job-"+tn, tn, 8, Resources{MilliCPU: CPU(1), MemBytes: GiB(2)}, false)
		}
		return h.cycle().Decisions
	}

	first := run()
	for i := 0; i < 8; i++ {
		got := run()
		if len(got) != len(first) {
			t.Fatalf("run %d placed %d tasks, first run placed %d", i, len(got), len(first))
		}
		for j := range got {
			if got[j].TaskID != first[j].TaskID || got[j].WorkerID != first[j].WorkerID {
				t.Fatalf("run %d decision %d: %s->%s, first run: %s->%s",
					i, j, got[j].TaskID, got[j].WorkerID, first[j].TaskID, first[j].WorkerID)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

func TestSubmissionRejectsNonsenseRequests(t *testing.T) {
	h := newHarness(t, DefaultConfig())
	h.addWorker("w1", Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)

	cases := []struct {
		name string
		req  Resources
	}{
		{"negative cpu", Resources{MilliCPU: -1, MemBytes: GiB(1)}},
		{"negative mem", Resources{MilliCPU: CPU(1), MemBytes: -1}},
		{"empty request", Resources{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			j := &Job{ID: "j-" + tc.name, Tenant: "acme", Tasks: []*Task{{ID: "t-" + tc.name, Request: tc.req}}}
			if err := h.s.Submit(j, h.now); err == nil {
				t.Errorf("accepted %s", tc.name)
			}
		})
	}
}

func TestSubmissionRejectsTaskLargerThanAnyWorker(t *testing.T) {
	// Catching this at submission turns a permanently pending task into an
	// immediate error.
	h := newHarness(t, DefaultConfig())
	h.addWorker("w1", Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(1000), MemBytes: GiB(2000)}, 1)

	j := &Job{ID: "huge", Tenant: "acme", Tasks: []*Task{
		{ID: "huge-t0", Request: Resources{MilliCPU: CPU(64), MemBytes: GiB(128)}},
	}}
	if err := h.s.Submit(j, h.now); err == nil {
		t.Fatal("accepted a task larger than the largest worker")
	}
}

func TestDuplicateSubmissionIsRejected(t *testing.T) {
	h := newHarness(t, DefaultConfig())
	h.addWorker("w1", Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}, "a")
	h.addTenant("acme", Resources{MilliCPU: CPU(100), MemBytes: GiB(200)}, 1)
	h.submit("j1", "acme", 1, Resources{MilliCPU: CPU(1), MemBytes: GiB(1)}, false)

	j := &Job{ID: "j1", Tenant: "acme", Tasks: []*Task{{ID: "other", Request: Resources{MilliCPU: CPU(1), MemBytes: GiB(1)}}}}
	if err := h.s.Submit(j, h.now); err == nil {
		t.Fatal("accepted a duplicate job id")
	}
}

func TestHeartbeatRejectsImpossibleReports(t *testing.T) {
	h := newHarness(t, DefaultConfig())
	cap := Resources{MilliCPU: CPU(4), MemBytes: GiB(8)}
	h.addWorker("w1", cap, "a")

	if err := h.s.registry.Heartbeat("w1", Resources{MilliCPU: CPU(8), MemBytes: GiB(16)}, nil, h.now); err == nil {
		t.Error("accepted a worker claiming more free capacity than it has")
	}
	if err := h.s.registry.Heartbeat("w1", Resources{MilliCPU: -1, MemBytes: GiB(1)}, nil, h.now); err == nil {
		t.Error("accepted a negative free-capacity report")
	}
	if err := h.s.registry.Heartbeat("unknown", cap, nil, h.now); err == nil {
		t.Error("accepted a heartbeat from an unregistered worker")
	}
}

func TestLedgerRejectsDoubleAssignment(t *testing.T) {
	l := NewLedger()
	a := Assignment{TaskID: "t1", WorkerID: "w1", Request: Resources{MilliCPU: CPU(1), MemBytes: GiB(1)}}
	if err := l.Reserve(a); err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	b := a
	b.WorkerID = "w2"
	if err := l.Reserve(b); err == nil {
		t.Fatal("reserved the same task twice: the first reservation would have leaked")
	}
	if err := l.checkInvariants(); err != nil {
		t.Errorf("invariants broken: %v", err)
	}
}

func TestLedgerDropsEmptyWorkerIndexes(t *testing.T) {
	// Without this, byWorker grows without bound across a long run as workers
	// come and go — a slow leak a short test never shows.
	l := NewLedger()
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("t%d", i)
		if err := l.Reserve(Assignment{TaskID: id, WorkerID: fmt.Sprintf("w%d", i),
			Request: Resources{MilliCPU: CPU(1), MemBytes: GiB(1)}}); err != nil {
			t.Fatalf("reserve: %v", err)
		}
		l.Release(id)
	}
	if n := len(l.byWorker); n != 0 {
		t.Errorf("byWorker retains %d empty maps", n)
	}
	if err := l.checkInvariants(); err != nil {
		t.Errorf("invariants broken: %v", err)
	}
}

func TestConfirmByTheWrongWorkerIsRejected(t *testing.T) {
	// Two copies may be running. Reported rather than silently re-pointed.
	l := NewLedger()
	if err := l.Reserve(Assignment{TaskID: "t1", WorkerID: "w1",
		Request: Resources{MilliCPU: CPU(1), MemBytes: GiB(1)}}); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := l.Confirm("t1", "w2"); err == nil {
		t.Fatal("accepted confirmation from a worker the task was not assigned to")
	}
	if err := l.Confirm("t1", "w1"); err != nil {
		t.Fatalf("rejected the correct worker: %v", err)
	}
	// Idempotent: a worker reports its full running set on every heartbeat.
	if err := l.Confirm("t1", "w1"); err != nil {
		t.Fatalf("second confirmation failed: %v", err)
	}
}

func TestFairnessIndexIgnoresIdleTenants(t *testing.T) {
	// Including idle tenants drags the index toward zero and makes a
	// perfectly fair busy cluster look unfair for having quiet tenants.
	busy := []Share{
		{Tenant: "a", Dominant: 0.5, Usage: Resources{MilliCPU: CPU(1)}},
		{Tenant: "b", Dominant: 0.5, Usage: Resources{MilliCPU: CPU(1)}},
		{Tenant: "idle"},
	}
	if idx := FairnessIndex(busy); idx < 0.99 {
		t.Errorf("fairness index = %.3f for two equal tenants plus an idle one, want ~1.0", idx)
	}

	skewed := []Share{
		{Tenant: "a", Dominant: 0.9, Usage: Resources{MilliCPU: CPU(1)}},
		{Tenant: "b", Dominant: 0.1, Usage: Resources{MilliCPU: CPU(1)}},
	}
	if idx := FairnessIndex(skewed); idx > 0.8 {
		t.Errorf("fairness index = %.3f for a 9:1 split, want well below 1", idx)
	}
}
