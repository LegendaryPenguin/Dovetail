package dovetail

import (
	"sort"
)

// Quota and fair share answer two different questions and the first version of
// this scheduler conflated them.
//
//	Quota is a ceiling. A tenant at quota does not get scheduled even on an
//	idle cluster. It exists so capacity planning is possible: a tenant's
//	maximum footprint is knowable in advance.
//
//	Fair share is an ordering. It decides who goes first when the cluster is
//	contended, and it is explicitly *not* a limit — a single tenant should be
//	able to use the whole cluster when nobody else wants it.
//
// Collapsing them produces one of two bad schedulers. Treat the share as a
// ceiling and the cluster idles while work queues. Treat the quota as a
// preference and one tenant takes everything, which is exactly the failure
// that made me write this file.

// QuotaCheck is why a tenant may or may not receive more capacity.
type QuotaCheck struct {
	Allowed bool
	// Headroom is quota minus current usage, clamped at zero.
	Headroom Resources
	Reason   string
}

// CheckQuota reports whether a tenant can take on another task.
//
// An unknown tenant is refused rather than given unlimited capacity. The
// alternative — treating a missing quota as infinite — means a typo in a
// tenant name silently creates an unbounded tenant, and the cluster finds out
// under load.
func CheckQuota(tenant Tenant, known bool, usage Resources, request Resources) QuotaCheck {
	if !known {
		return QuotaCheck{Allowed: false, Reason: "unknown tenant"}
	}
	headroom := tenant.Quota.Sub(usage).Clamp()
	if !request.Fits(headroom) {
		return QuotaCheck{
			Allowed:  false,
			Headroom: headroom,
			Reason:   "tenant is at quota (headroom " + headroom.String() + ", request " + request.String() + ")",
		}
	}
	return QuotaCheck{Allowed: true, Headroom: headroom}
}

// Share is a tenant's position in the fairness ordering.
type Share struct {
	Tenant string
	// Dominant is the tenant's dominant resource share: the largest fraction
	// of any single cluster resource it occupies. See Resources.DominantShare.
	Dominant float64
	// Weighted is Dominant divided by the tenant's weight, and is what the
	// ordering actually sorts on. Dividing rather than multiplying is the
	// correct direction and took me a moment: a tenant with weight 3 should
	// be *allowed* three times the share before it looks equally greedy, so
	// its effective greediness is its share scaled down.
	Weighted float64
	// Dimension names which resource was dominant, for operator reporting.
	// A cluster where every tenant's dominant resource is memory is a cluster
	// that needs more memory, and a single share number hides that.
	Dimension string
	Usage     Resources
	Demand    Resources
}

// ComputeShares returns every tenant's dominant resource share.
//
// Tenants with demand but no usage get a share of zero and therefore sort
// first, which is what gives a newly arriving tenant immediate access to
// contested capacity rather than making it wait out whoever is already there.
func ComputeShares(
	tenants map[string]Tenant,
	usage map[string]Resources,
	demand map[string]Resources,
	clusterTotal Resources,
) []Share {
	// Union of tenants that are configured, using, or asking. A tenant using
	// capacity after being deconfigured still has to appear, or its usage
	// vanishes from the fairness picture while continuing to occupy machines.
	seen := make(map[string]struct{})
	for id := range tenants {
		seen[id] = struct{}{}
	}
	for id := range usage {
		seen[id] = struct{}{}
	}
	for id := range demand {
		seen[id] = struct{}{}
	}

	out := make([]Share, 0, len(seen))
	for id := range seen {
		u := usage[id]
		share, dim := u.DominantShare(clusterTotal)
		weight := 1.0
		if t, ok := tenants[id]; ok {
			weight = t.EffectiveWeight()
		}
		out = append(out, Share{
			Tenant:    id,
			Dominant:  share,
			Weighted:  share / weight,
			Dimension: dim,
			Usage:     u,
			Demand:    demand[id],
		})
	}

	// Least-weighted-share first. Ties broken by name so that a scheduling
	// cycle is deterministic: without it, Go's map iteration order makes two
	// runs on identical input place work differently, which turns every
	// simulator comparison into noise and every bug into a heisenbug.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Weighted != out[j].Weighted {
			return out[i].Weighted < out[j].Weighted
		}
		return out[i].Tenant < out[j].Tenant
	})
	return out
}

// OrderTenants returns tenant IDs in fair-share order, skipping any with no
// pending demand.
//
// Skipping zero-demand tenants matters for more than efficiency. A tenant with
// no work has a share of zero and would otherwise sort to the front of every
// cycle, and a scheduler that keeps offering capacity to tenants that do not
// want it makes no progress at all.
func OrderTenants(shares []Share) []string {
	out := make([]string, 0, len(shares))
	for _, s := range shares {
		if s.Demand.IsZero() {
			continue
		}
		out = append(out, s.Tenant)
	}
	return out
}

// FairnessIndex returns Jain's fairness index over tenants' dominant shares,
// in (0, 1]. One means perfectly equal shares.
//
// Used by the simulator to compare policies with a single number. Its
// weakness is worth stating because it is easy to quote misleadingly: it
// measures equality of *allocation*, not of *satisfaction*. A cluster where
// every tenant gets exactly 10% scores a perfect 1.0 whether they each wanted
// 10% or each wanted 90%. Read it alongside queue latency, never instead.
func FairnessIndex(shares []Share) float64 {
	var sum, sumSquares float64
	n := 0
	for _, s := range shares {
		// Only tenants that are actually participating. Including idle
		// tenants with zero share drags the index toward zero and makes a
		// perfectly fair busy cluster look unfair for having quiet tenants.
		if s.Usage.IsZero() && s.Demand.IsZero() {
			continue
		}
		sum += s.Dominant
		sumSquares += s.Dominant * s.Dominant
		n++
	}
	if n == 0 || sumSquares == 0 {
		return 1
	}
	return (sum * sum) / (float64(n) * sumSquares)
}
