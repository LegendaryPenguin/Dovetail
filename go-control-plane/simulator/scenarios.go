package simulator

import (
	"time"

	dovetail "github.com/LegendaryPenguin/dovetail"
)

// Each scenario answers one question, stated above it. A scenario that does
// not have a question attached produces numbers nobody can act on, which was
// true of my first three.

func cpu(n float64) int64 { return dovetail.CPU(n) }
func gib(n float64) int64 { return dovetail.GiB(n) }

func res(c, m float64) dovetail.Resources {
	return dovetail.Resources{MilliCPU: cpu(c), MemBytes: gib(m)}
}

// baseFleet is 20 workers of 8 CPU / 32 GiB across 3 zones — 160 CPU and
// 640 GiB total.
func baseFleet(sc *Scenario) {
	sc.Workers = 20
	sc.WorkerCapacity = res(8, 32)
	sc.Zones = []string{"a", "b", "c"}
	sc.CycleInterval = 250 * time.Millisecond
	sc.Duration = 2 * time.Minute
	sc.Seed = 0xD0FE7A11
}

// Question: does quota actually protect a small tenant from a greedy one, or
// does it just cap the greedy one after the damage?
//
// The comparison is the point: the same workload with and without a binding
// quota on the greedy tenant.
func QuotaProtection() []Scenario {
	unbounded := Scenario{Name: "quota: greedy tenant unbounded", Config: dovetail.DefaultConfig()}
	baseFleet(&unbounded)
	unbounded.Tenants = []TenantProfile{
		{
			ID: "greedy", Quota: res(1000, 4000), Weight: 1,
			SubmitRate: 6, TasksPerJobMin: 4, TasksPerJobMax: 12,
			Request: res(1, 4), DurationMean: 4 * time.Second,
		},
		{
			ID: "small", Quota: res(1000, 4000), Weight: 1,
			SubmitRate: 0.5, TasksPerJobMin: 1, TasksPerJobMax: 2,
			Request: res(1, 4), DurationMean: 4 * time.Second,
		},
	}

	bounded := unbounded
	bounded.Name = "quota: greedy tenant capped at 50%"
	bounded.Tenants = append([]TenantProfile(nil), unbounded.Tenants...)
	// Half the fleet: 80 CPU of 160.
	bounded.Tenants[0].Quota = res(80, 320)

	return []Scenario{unbounded, bounded}
}

// Question: does fair-share weighting do what it claims under contention, and
// does it hold when one tenant's tasks are much larger than the other's?
//
// The second half matters because dominant-resource fairness is supposed to
// handle heterogeneous shapes, and a scheduler that only works when everyone
// asks for the same thing has not solved the problem.
func WeightedFairness() []Scenario {
	equal := Scenario{Name: "fairness: equal weights, equal shapes", Config: dovetail.DefaultConfig()}
	baseFleet(&equal)
	equal.Tenants = []TenantProfile{
		{ID: "a", Quota: res(1000, 4000), Weight: 1, SubmitRate: 5,
			TasksPerJobMin: 4, TasksPerJobMax: 8, Request: res(1, 4), DurationMean: 5 * time.Second},
		{ID: "b", Quota: res(1000, 4000), Weight: 1, SubmitRate: 5,
			TasksPerJobMin: 4, TasksPerJobMax: 8, Request: res(1, 4), DurationMean: 5 * time.Second},
	}

	weighted := equal
	weighted.Name = "fairness: 3:1 weights"
	weighted.Tenants = append([]TenantProfile(nil), equal.Tenants...)
	weighted.Tenants[0].Weight = 3

	// Heterogeneous shapes: one tenant is CPU-bound, the other memory-bound.
	// DRF should let both get a meaningful share, which a single-resource
	// comparison cannot express.
	shapes := equal
	shapes.Name = "fairness: cpu-bound vs memory-bound tenants"
	shapes.Tenants = []TenantProfile{
		{ID: "cpu-heavy", Quota: res(1000, 4000), Weight: 1, SubmitRate: 5,
			TasksPerJobMin: 4, TasksPerJobMax: 8, Request: res(4, 2), DurationMean: 5 * time.Second},
		{ID: "mem-heavy", Quota: res(1000, 4000), Weight: 1, SubmitRate: 5,
			TasksPerJobMin: 4, TasksPerJobMax: 8, Request: res(1, 16), DurationMean: 5 * time.Second},
	}

	return []Scenario{equal, weighted, shapes}
}

// Question: how much does packing bias actually matter?
//
// I expected this to be one of the more consequential knobs. The answer in
// bench/results.md is that it is not, and that finding is more useful than a
// tuned default would have been.
func PackingBias() []Scenario {
	var out []Scenario
	for _, bias := range []float64{0.0, 0.35, 0.7, 1.0} {
		cfg := dovetail.DefaultConfig()
		cfg.PackingBias = bias
		sc := Scenario{
			Name:   "packing bias " + formatFloat(bias),
			Config: cfg,
		}
		baseFleet(&sc)
		sc.Tenants = []TenantProfile{
			{ID: "mixed-a", Quota: res(1000, 4000), Weight: 1, SubmitRate: 4,
				TasksPerJobMin: 2, TasksPerJobMax: 10, Request: res(2, 4), DurationMean: 6 * time.Second},
			{ID: "mixed-b", Quota: res(1000, 4000), Weight: 1, SubmitRate: 4,
				TasksPerJobMin: 1, TasksPerJobMax: 4, Request: res(1, 12), DurationMean: 3 * time.Second},
		}
		out = append(out, sc)
	}
	return out
}

// Question: what does a worker failure cost, and does the cluster leak
// capacity across one?
//
// Two failure shapes, because they exercise different paths. A removed worker
// is the easy case — it vanishes and the registry notices. A *stalled* worker
// is the realistic one: still registered, still holding assignments, silently
// not doing anything, and the control plane has to work it out from absence.
func WorkerFailure() []Scenario {
	base := Scenario{Name: "failure: baseline, no failure", Config: dovetail.DefaultConfig()}
	baseFleet(&base)
	base.Tenants = []TenantProfile{
		{ID: "steady", Quota: res(1000, 4000), Weight: 1, SubmitRate: 6,
			TasksPerJobMin: 3, TasksPerJobMax: 6, Request: res(1, 4), DurationMean: 5 * time.Second},
	}

	removed := base
	removed.Name = "failure: worker removed at 1m"
	removed.KillWorkerAt = time.Minute

	stalled := base
	stalled.Name = "failure: worker stops heartbeating at 1m"
	stalled.StallWorkerAt = time.Minute

	return []Scenario{base, removed, stalled}
}

// Question: do gangs get starved, and what does gang scheduling cost the
// tenants around them?
//
// There is no preemption, so a large gang waits for natural completions. The
// expected answer is that gang latency is much worse than single-task latency
// and that the gap widens with gang size — which is a limitation to measure
// rather than hide.
func GangScheduling() []Scenario {
	none := Scenario{Name: "gangs: none", Config: dovetail.DefaultConfig()}
	baseFleet(&none)
	none.Tenants = []TenantProfile{
		{ID: "batch", Quota: res(1000, 4000), Weight: 1, SubmitRate: 5,
			TasksPerJobMin: 4, TasksPerJobMax: 8, Request: res(1, 4), DurationMean: 5 * time.Second},
	}

	some := none
	some.Name = "gangs: 30% of jobs are gangs"
	some.Tenants = append([]TenantProfile(nil), none.Tenants...)
	some.Tenants[0].GangFraction = 0.3

	large := none
	large.Name = "gangs: 30% gangs, 8-16 tasks each"
	large.Tenants = append([]TenantProfile(nil), none.Tenants...)
	large.Tenants[0].GangFraction = 0.3
	large.Tenants[0].TasksPerJobMin = 8
	large.Tenants[0].TasksPerJobMax = 16

	return []Scenario{none, some, large}
}

// Question: how does scheduling interval trade queue latency against work
// done per cycle?
func CycleInterval() []Scenario {
	var out []Scenario
	for _, interval := range []time.Duration{
		100 * time.Millisecond, 250 * time.Millisecond, 1 * time.Second, 2 * time.Second,
	} {
		sc := Scenario{Name: "cycle interval " + interval.String(), Config: dovetail.DefaultConfig()}
		baseFleet(&sc)
		sc.CycleInterval = interval
		sc.Tenants = []TenantProfile{
			{ID: "steady", Quota: res(1000, 4000), Weight: 1, SubmitRate: 15,
				TasksPerJobMin: 3, TasksPerJobMax: 6, Request: res(1, 4), DurationMean: 5 * time.Second},
		}
		out = append(out, sc)
	}
	return out
}

// All returns every scenario group, in reporting order.
func All() map[string][]Scenario {
	return map[string][]Scenario{
		"quota":    QuotaProtection(),
		"fairness": WeightedFairness(),
		"packing":  PackingBias(),
		"failure":  WorkerFailure(),
		"gangs":    GangScheduling(),
		"cycle":    CycleInterval(),
	}
}

// GroupOrder is the order to report groups in, since Go maps do not have one.
var GroupOrder = []string{"quota", "fairness", "packing", "failure", "gangs", "cycle"}

func formatFloat(f float64) string {
	switch f {
	case 0:
		return "0.0 (spread)"
	case 1:
		return "1.0 (pack)"
	default:
		return trimFloat(f)
	}
}

func trimFloat(f float64) string {
	s := []byte("0.00")
	s[2] = byte('0' + int(f*10)%10)
	s[3] = byte('0' + int(f*100)%10)
	return string(s)
}
