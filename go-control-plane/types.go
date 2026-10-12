package dovetail

import (
	"fmt"
	"sort"
	"time"
)

// Phase is where a task is in its lifecycle.
//
// The states that matter are the two in the middle. Assigned means the
// control plane has decided and told nobody yet or is waiting to be told it
// landed; Running means a worker has confirmed it. Collapsing those two is the
// single easiest way to double-book a machine, and keeping them apart is what
// the reservation ledger in reservations.go is for.
type Phase string

const (
	// PhasePending: queued, not yet placed.
	PhasePending Phase = "pending"
	// PhaseAssigned: the scheduler has committed capacity on a worker, but no
	// heartbeat has yet confirmed the task is running. Capacity is reserved.
	PhaseAssigned Phase = "assigned"
	// PhaseRunning: a worker heartbeat has reported it. The worker's own
	// free-resource report now accounts for it.
	PhaseRunning   Phase = "running"
	PhaseSucceeded Phase = "succeeded"
	PhaseFailed    Phase = "failed"
	// PhaseLost: the worker holding it stopped heartbeating. Distinct from
	// Failed because the task may well still be running out there — we simply
	// cannot see it. The distinction drives whether a retry is safe.
	PhaseLost Phase = "lost"
)

func (p Phase) Terminal() bool {
	return p == PhaseSucceeded || p == PhaseFailed
}

// Occupies reports whether a task in this phase is holding capacity on a
// worker from the scheduler's point of view.
func (p Phase) Occupies() bool {
	return p == PhaseAssigned || p == PhaseRunning
}

// Task is one unit of placeable work.
type Task struct {
	ID      string
	JobID   string
	Tenant  string
	Request Resources

	Phase    Phase
	WorkerID string

	// Attempts counts how many times this task has been placed. Bounded by
	// the job's MaxAttempts so a task that fails deterministically cannot
	// occupy the scheduler forever.
	Attempts int

	// AssignedAt is when the control plane committed capacity. Used to expire
	// a reservation whose worker never confirmed it — see reservations.go.
	AssignedAt time.Time
	StartedAt  time.Time
	EndedAt    time.Time

	// LastError is the most recent failure reason, kept for the operator
	// rather than for any scheduling decision.
	LastError string
}

// Job is a group of tasks submitted together.
type Job struct {
	ID       string
	Tenant   string
	Priority int
	// Gang, when true, means the job's tasks must all start together or none
	// of them do. See gang.go: this is not a preference, and a scheduler that
	// treats it as one will deadlock a cluster with half-placed gangs.
	Gang bool
	// MaxAttempts bounds per-task retries. Zero means the package default.
	MaxAttempts int
	SubmittedAt time.Time

	Tasks []*Task
}

// Pending returns the job's tasks that still need placing.
func (j *Job) Pending() []*Task {
	var out []*Task
	for _, t := range j.Tasks {
		if t.Phase == PhasePending {
			out = append(out, t)
		}
	}
	return out
}

// Demand is the total request of every task not yet finished.
//
// Includes running tasks on purpose: fair-share accounting compares what
// tenants are *using*, and a tenant whose work is all running is using the
// cluster just as much as one whose work is all queued.
func (j *Job) Demand() Resources {
	var total Resources
	for _, t := range j.Tasks {
		if !t.Phase.Terminal() {
			total = total.Add(t.Request)
		}
	}
	return total
}

func (j *Job) Complete() bool {
	for _, t := range j.Tasks {
		if !t.Phase.Terminal() {
			return false
		}
	}
	return true
}

// Tenant is a unit of quota and fairness.
type Tenant struct {
	ID string
	// Quota is a hard ceiling. A tenant at quota is not scheduled even if the
	// cluster is otherwise idle, which is the point of a quota as opposed to a
	// share: it makes capacity planning possible by making a tenant's maximum
	// footprint predictable.
	Quota Resources
	// Weight scales the tenant's fair share. Two tenants with weights 1 and 3
	// converge toward 25%/75% of contested capacity. Zero or negative is
	// treated as 1.
	Weight float64
}

func (t Tenant) EffectiveWeight() float64 {
	if t.Weight <= 0 {
		return 1
	}
	return t.Weight
}

// Topology is where a worker sits. Placement can prefer spreading across
// zones or packing within one.
type Topology struct {
	Zone string
	Rack string
}

// Worker is one machine, as the control plane understands it.
//
// The three capacity fields are the heart of this project and are kept
// separate on purpose:
//
//   - Capacity is the machine's total, from registration. Static.
//   - Reported is what the worker's last heartbeat said was free. It already
//     accounts for everything the worker knows it is running — including work
//     the control plane has forgotten about, which is why reconciliation
//     exists.
//   - Assignments is what the control plane believes it has placed here.
//
// Available capacity is *not* Reported. It is Reported minus the reservations
// the worker has not yet confirmed, because the heartbeat predates them. That
// subtraction is the difference between a scheduler that works and one that
// cheerfully places two tasks into the same gap.
type Worker struct {
	ID       string
	Capacity Resources
	Reported Resources
	Topology Topology

	RegisteredAt  time.Time
	LastHeartbeat time.Time

	// Draining workers accept no new work but keep what they have. Set by an
	// operator ahead of a planned shutdown.
	Draining bool

	// RunningTaskIDs is what the worker itself claims to be running, from its
	// last heartbeat. Compared against the control plane's own view by the
	// reconciler; a disagreement in either direction is a real event and not
	// a transient.
	RunningTaskIDs []string
}

// Healthy reports whether the worker has heartbeated recently enough to place
// work on.
//
// A warming worker — registered, no heartbeat yet — is deliberately *not*
// healthy. Placing work on a machine that has never reported is placing it on
// faith, and the failure shows up as a lost task several timeouts later rather
// than as a scheduling decision that was wrong at the time.
func (w *Worker) Healthy(now time.Time, timeout time.Duration) bool {
	if w.LastHeartbeat.IsZero() {
		return false
	}
	return now.Sub(w.LastHeartbeat) <= timeout
}

// Stale reports whether the worker has gone quiet past the timeout.
//
// Not simply !Healthy: a warming worker is neither healthy nor stale, and
// conflating the two means a worker gets reaped during the window between
// registering and its first heartbeat, having its (nonexistent) assignments
// returned and being counted as a failure. The simulator found that one.
func (w *Worker) Stale(now time.Time, timeout time.Duration) bool {
	if w.LastHeartbeat.IsZero() {
		return now.Sub(w.RegisteredAt) > timeout
	}
	return now.Sub(w.LastHeartbeat) > timeout
}

// Warming reports whether the worker has registered but not yet reported.
func (w *Worker) Warming() bool { return w.LastHeartbeat.IsZero() }

// Assignment is the control plane's record of a task placed on a worker.
type Assignment struct {
	TaskID   string
	WorkerID string
	Tenant   string
	Request  Resources
	// Confirmed is set once a heartbeat has reported the task running, at
	// which point the worker's Reported figure accounts for it and the
	// reservation stops being subtracted separately.
	Confirmed bool
	CreatedAt time.Time
	// Epoch is the scheduling cycle that created this assignment, used to
	// make retries idempotent: a worker replaying an old assignment command
	// can be told it is stale rather than silently starting a second copy.
	Epoch uint64
}

// Decision is one placement the scheduler wants to make.
type Decision struct {
	TaskID   string
	WorkerID string
	Tenant   string
	Request  Resources
	// Score the winning worker received, kept so the simulator can report
	// why placements went where they did rather than only that they did.
	Score float64
	// Reason explains a *rejection* when WorkerID is empty. A scheduler that
	// cannot say why a task did not place is a scheduler nobody can operate.
	Reason string
}

func (d Decision) Placed() bool { return d.WorkerID != "" }

// Config is the scheduler's tunables.
type Config struct {
	// HeartbeatTimeout is how long a worker may go quiet before its
	// assignments are considered lost.
	HeartbeatTimeout time.Duration
	// ReservationTimeout is how long an assigned-but-unconfirmed task holds
	// capacity before the scheduler gives up on it.
	//
	// Separate from HeartbeatTimeout and necessarily shorter. A worker that is
	// alive and heartbeating but never reports a particular task running has
	// lost the command; waiting a full heartbeat timeout for that would strand
	// capacity for no reason.
	ReservationTimeout time.Duration
	// MaxAttempts bounds per-task retries.
	MaxAttempts int
	// PackingBias in [0,1] trades consolidation against spreading. 1 packs
	// hardest; 0 spreads. Measured in bench/results.md — the honest finding is
	// that it matters far less than quota does.
	PackingBias float64
	// SpreadGangsAcrossZones makes gang placement prefer distinct zones,
	// trading locality for failure independence.
	SpreadGangsAcrossZones bool
	// MaxUnitsPerCycle bounds how deep into each tenant's queue one cycle
	// looks. Zero means the package default.
	//
	// A cycle cannot place more than the cluster has room for, so considering
	// an arbitrarily deep backlog is wasted work — and enough of it to make
	// the control plane itself the bottleneck once a queue reaches tens of
	// thousands of tasks. The cap applies after the priority sort, so a cycle
	// always sees the head of each queue.
	MaxUnitsPerCycle int
}

func DefaultConfig() Config {
	return Config{
		HeartbeatTimeout:   15 * time.Second,
		ReservationTimeout: 5 * time.Second,
		MaxAttempts:        3,
		PackingBias:        0.7,
		MaxUnitsPerCycle:   512,
	}
}

func (c Config) withDefaults() Config {
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = 15 * time.Second
	}
	if c.ReservationTimeout <= 0 {
		c.ReservationTimeout = 5 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.PackingBias < 0 {
		c.PackingBias = 0
	}
	if c.PackingBias > 1 {
		c.PackingBias = 1
	}
	if c.MaxUnitsPerCycle <= 0 {
		c.MaxUnitsPerCycle = 512
	}
	return c
}

// Validate reports a configuration that cannot work.
func (c Config) Validate() error {
	if c.ReservationTimeout >= c.HeartbeatTimeout {
		// If a reservation outlives the heartbeat window, a worker that dies
		// holding an unconfirmed assignment has its capacity returned by the
		// staleness path rather than the reservation path, and the
		// reservation expiry becomes dead code that still looks live.
		return fmt.Errorf("ReservationTimeout (%s) must be shorter than HeartbeatTimeout (%s)",
			c.ReservationTimeout, c.HeartbeatTimeout)
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
