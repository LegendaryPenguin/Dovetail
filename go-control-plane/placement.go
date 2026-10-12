package dovetail

import (
	"math"
	"sort"
	"time"
)

// Placement runs in three stages that stay separate: eligibility, filtering,
// scoring.
//
// Keeping them apart is the design decision I would defend hardest in this
// package, and it is not about tidiness. Each stage answers a different kind
// of question and the answers compose badly when mixed:
//
//	Eligibility  may this task run at all right now? (quota, attempts, gang)
//	             — a property of the task and its tenant, no worker involved.
//	Filtering    which workers *can* run it? (resources, health, draining)
//	             — a hard predicate. Yes or no, never a preference.
//	Scoring      which of those *should* run it? (packing, spread, topology)
//	             — a preference. Never load-bearing for correctness.
//
// The first version folded filtering into scoring by giving infeasible workers
// a very low score. It placed tasks on workers that could not run them the
// moment every score went negative, because "least bad" is still a winner when
// nothing is good. A filter that returns no candidates cannot do that.

// Filter reasons, as constants because they end up in operator-facing output
// and in the simulator's per-cycle accounting.
const (
	ReasonNoWorkers      = "no workers registered"
	ReasonAllUnhealthy   = "no healthy workers"
	ReasonInsufficient   = "no worker has sufficient free capacity"
	ReasonQuotaExceeded  = "tenant is at quota"
	ReasonAttemptsBurned = "task has exhausted its attempts"
	ReasonGangInfeasible = "gang cannot be placed atomically"
	ReasonDraining       = "all candidate workers are draining"
)

// FilterResult is which workers survived, and a tally of why the others did
// not.
//
// The tally is not decoration. "Nothing placed" is the single most common
// operator question, and the difference between "every worker was full" and
// "every worker was unhealthy" is the difference between adding capacity and
// paging whoever broke the heartbeats.
type FilterResult struct {
	Feasible []*Worker
	Rejected map[string]int
}

func (f *FilterResult) reject(reason string) {
	if f.Rejected == nil {
		f.Rejected = make(map[string]int)
	}
	f.Rejected[reason]++
}

// DominantReason returns the most common rejection, for the single-line
// explanation attached to an unplaced task.
func (f *FilterResult) DominantReason() string {
	best, bestN := ReasonInsufficient, -1
	for _, k := range sortedKeys(f.Rejected) {
		if n := f.Rejected[k]; n > bestN {
			best, bestN = k, n
		}
	}
	return best
}

// FilterWorkers returns the workers that can actually run the request.
//
// Every check here is a hard predicate. Nothing in this function expresses a
// preference, and anything that wants to must go in Score instead.
func FilterWorkers(workers []*Worker, ledger *Ledger, request Resources, ctx SchedulingContext) FilterResult {
	result := FilterResult{}
	for _, w := range workers {
		switch {
		case w.Draining:
			// Keeps what it has, accepts nothing new.
			result.reject(ReasonDraining)
		case !w.Healthy(ctx.Now, ctx.Config.HeartbeatTimeout):
			// Includes warming workers, which have registered but never
			// reported. Placing work on a machine that has never said
			// anything is placing it on faith.
			result.reject(ReasonAllUnhealthy)
		case !request.Fits(ledger.Available(w)):
			// Available, not Reported. The difference is the unconfirmed
			// reservations this cycle has already made — see reservations.go
			// for the double-booking bug that taught me to write it this way.
			result.reject(ReasonInsufficient)
		default:
			result.Feasible = append(result.Feasible, w)
		}
	}
	return result
}

// SchedulingContext is the state a scheduling decision is made against.
//
// Passed explicitly rather than read from the scheduler so that placement is
// a pure function of its inputs, which is what makes the exhaustive tests in
// placement_test.go possible without constructing a whole control plane.
type SchedulingContext struct {
	Now          time.Time
	Config       Config
	ClusterTotal Resources
	// ZoneLoad is the fraction of each zone's capacity currently committed,
	// used by the spread term in scoring.
	ZoneLoad map[string]float64
	// TenantZoneCount is how many of the tenant's tasks already sit in each
	// zone, used to spread a single tenant's work across failure domains.
	TenantZoneCount map[string]int
}

// Score ranks a feasible worker. Higher is better.
//
// Three terms, deliberately few. Every scoring term is a knob someone will
// eventually tune in the wrong direction, and a scorer with a dozen terms is
// one nobody can predict. The weights are in Config so the simulator can
// sweep them, and bench/results.md reports the honest finding: packing bias
// changes utilisation by a few percent while quota configuration changes it by
// tens of percent. The scorer is not where the leverage is.
func Score(w *Worker, ledger *Ledger, request Resources, tenant string, ctx SchedulingContext) float64 {
	available := ledger.Available(w)

	// Term 1 — packing. The fraction of the worker that would be in use after
	// this placement. High means consolidated.
	//
	// Computed against Capacity rather than Reported so that a worker which
	// is already busy scores as busy. Using Reported would make a nearly-full
	// worker look empty, since its *reported free* capacity is small and the
	// ratio would be taken against the wrong denominator.
	afterUse := w.Capacity.Sub(available.Sub(request))
	packCPU, packMem := afterUse.Utilization(w.Capacity)
	pack := math.Max(packCPU, packMem)

	// Term 2 — balance. Penalise leaving a worker lopsided: lots of CPU free
	// and no memory, or the reverse.
	//
	// This matters more than it looks. A cluster of workers each with 8 cores
	// and 200 MiB free has plenty of aggregate CPU and can schedule nothing,
	// and the only way to avoid arriving there is to prefer placements that
	// keep the remaining vector proportional.
	remaining := available.Sub(request)
	remCPU, remMem := remaining.Utilization(w.Capacity)
	balance := 1 - math.Abs(remCPU-remMem)

	// Term 3 — spread. Prefer zones where this tenant has less work, so a
	// zone failure does not take out everything one tenant is running.
	spread := 1.0
	if n, ok := ctx.TenantZoneCount[tenant+"/"+w.Topology.Zone]; ok && n > 0 {
		// Diminishing penalty: the difference between 0 and 1 task in a zone
		// matters much more than between 10 and 11.
		spread = 1 / (1 + math.Log1p(float64(n)))
	}

	bias := ctx.Config.PackingBias
	// Packing and spreading pull against each other, so they share the bias.
	// Balance is weighted independently and lightly because it is a tiebreak
	// among placements that are already acceptable, not a goal in itself.
	return bias*pack + (1-bias)*spread + 0.25*balance
}

// SelectWorker picks the best feasible worker, or nil.
//
// Ties broken by worker ID. Determinism is not cosmetic here: without it, two
// simulator runs on identical input diverge, every A/B comparison between
// policies becomes noise, and a reproduction of a scheduling bug stops
// reproducing.
func SelectWorker(feasible []*Worker, ledger *Ledger, request Resources, tenant string, ctx SchedulingContext) (*Worker, float64) {
	var best *Worker
	bestScore := math.Inf(-1)
	for _, w := range feasible {
		s := Score(w, ledger, request, tenant, ctx)
		if s > bestScore || (s == bestScore && best != nil && w.ID < best.ID) {
			best, bestScore = w, s
		}
	}
	if best == nil {
		return nil, 0
	}
	return best, bestScore
}

// SortWorkersByID gives placement a stable iteration order.
//
// Callers hold workers in a map, and Go randomises map iteration. Sorting at
// the boundary is cheaper than reasoning about which downstream behaviours
// depend on order, and it means a filter tally is reproducible.
func SortWorkersByID(workers []*Worker) {
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
}
