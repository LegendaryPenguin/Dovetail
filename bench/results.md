# Simulation results

Produced by `cd go-control-plane && go run ./cmd/dovetail-sim`. Apple Silicon,
Go 1.27.

**Method, stated so the numbers can be discounted appropriately.** The
simulator drives the *real* scheduler — nothing here reimplements placement —
against a synthetic fleet on a virtual clock, so a 2-minute simulation runs in
milliseconds and every run is reproducible from its seed. Task durations are
drawn from an exponential distribution I chose, not measured from anything, so
**absolute numbers describe the generator**. The comparisons between
configurations are the output worth reading.

Fleet throughout: 20 workers of 8 CPU / 32 GiB across 3 zones — 160 CPU, 640
GiB. Cycle interval 250 ms, 2 simulated minutes (480 cycles) unless noted.

One caveat that applies to every row: the simulator generates each worker's
heartbeat from the control plane's own view of what it assigned, so a worker
can only disagree with the control plane through the explicitly injected
failures. Genuine divergence — a worker running something nobody asked for —
is covered by unit tests, and **the orphan-adoption path is therefore
under-exercised here**.

---

## 1. Quota protects nothing that fair share was not already protecting

| | greedy completions | small completions | small p95 queue | CPU util |
|---|---|---|---|---|
| greedy unbounded | 4,258 | 99 | **0s** | **92.5%** |
| greedy capped at 50% | 2,205 | 99 | **0s** | **48.6%** |

This is the result I did not expect and it is the most useful thing in the
file.

The small tenant submits 0.5 jobs/sec against the greedy tenant's 6. I added
quotas specifically because the first scheduler let a greedy tenant lock out a
small one — and with fair share in place, **the quota changes the small
tenant's experience not at all**. Its p95 queue latency is 0s either way, and
its completion count is identical.

What the quota *did* do is idle half the cluster. Capping greedy at 50% took
utilisation from 92.5% to 48.6% and halved total throughput, because there was
nobody else to use the capacity the cap withheld.

The mechanism is in `fairshare.go`: a tenant with zero usage has a dominant
share of zero and sorts first in every cycle, so the small tenant is served
*immediately* on arrival regardless of how much work the greedy tenant has
queued. Fair share already solves the starvation problem. Quota solves a
different one — making a tenant's maximum footprint predictable for capacity
planning — and conflating the two is what made me expect this table to look
different.

**Honest reading:** quota is the wrong tool for starvation and the right tool
for predictability. On a cluster with no other demand it is pure waste.

---

## 2. Weighted fair share works, including across resource shapes

| scenario | tenant A | tenant B | fairness index |
|---|---|---|---|
| equal weights, equal shapes | 1,783 | 1,774 | 0.999 |
| **3:1 weights** | **2,661 (74%)** | **935 (26%)** | 0.809 |
| cpu-bound vs memory-bound | 465 | 462 | 0.997 |

The 3:1 row lands at 74%/26% against a predicted 75%/25%. DRF equilibrium is
where `heavy_share / 3 == light_share / 1` with the two summing to the full
cluster, so 0.75 and 0.25; the 1% gap is integral task sizes.

Queue latency splits the way the weights imply: tenant A's p95 is 26.75s
against B's 1m26s. The weight buys latency, not just throughput.

**The third row is the one that justifies DRF over simpler schemes.** One
tenant asks for 4 CPU / 2 GiB per task, the other 1 CPU / 16 GiB — opposite
shapes. Both get within 1% of the same completion count. A scheduler comparing
tenants on CPU alone would have handed the memory-bound tenant far too much,
and one comparing on memory alone the reverse. Comparing *dominant* shares is
what makes "half the cluster each" meaningful when the two halves are different
shapes.

That row also exposes a cost: utilisation drops to 59.5% CPU / 53.2% memory,
against 94% in the homogeneous case. Mixed shapes fragment the cluster — a
worker with 4 CPU and 200 MiB free can run neither tenant's tasks. The
balance term in `Score` is aimed at this and clearly does not solve it.

---

## 3. Packing bias does essentially nothing

| bias | CPU util | mem util | p50 queue | p95 queue |
|---|---|---|---|---|
| 0.0 (spread) | 76.0% | 82.1% | 1.25s | 1m8.25s |
| 0.35 | 76.0% | 82.2% | 1.25s | 1m8.5s |
| 0.70 | 76.0% | 82.2% | 1s | 1m8.5s |
| 1.0 (pack) | 76.0% | 82.2% | 1s | 1m8.5s |

Identical to three significant figures across the full range of the knob.

I expected this to be one of the more consequential settings and it is inert.
The reason, in hindsight: with 20 homogeneous workers and tasks much smaller
than a worker, almost every feasible worker is equally good, so the scoring
term that distinguishes them has almost nothing to distinguish. Packing bias
would start to matter with heterogeneous workers, tasks approaching worker
size, or a scale-to-zero policy that rewards consolidation — none of which this
fleet has.

**A negative result is still a result.** The honest conclusion is that tuning
this knob is not where the leverage is, and that §1's quota configuration
changes utilisation by 44 points while this changes it by 0.1.

---

## 4. Worker failure costs seconds, and leaks nothing

| | completions | p95 queue | workers lost | tasks lost | retried | reservations lapsed |
|---|---|---|---|---|---|---|
| baseline | 3,134 | 0s | 0 | 0 | 0 | 0 |
| worker removed at 1m | 3,124 | 500ms | 0 | 0 | 0 | 0 |
| **worker stops heartbeating at 1m** | 3,099 | **1.25s** | 1 | 8 | 12 | 4 |

The two failure shapes exercise different paths and the difference between
their rows is the interesting part.

**A removed worker costs almost nothing** — 10 completions out of 3,134 — and
reports zero workers lost. That zero is an artifact, not a finding: the
simulator removes the worker from the registry directly, so the staleness
reaper never sees it. It measures the capacity loss and not the detection
cost.

**A stalled worker is the realistic failure and the one with a measurable
price.** Still registered, still holding assignments, silently doing nothing.
The control plane has to infer it from absence, which takes a heartbeat timeout,
and the cost shows up as p95 queue latency going from 500ms to 1.25s plus 8
tasks requeued.

The `reservations lapsed: 4` column is the reservation timeout earning its
existence. Those are assignments made to the stalled worker *after* it stopped
responding but *before* it was declared stale — capacity that would have sat
stranded for a full heartbeat timeout if the shorter reservation timeout did
not exist.

**Nothing leaks.** The accounting audit — which cross-checks the ledger against
task phases independently — passes on every cycle of every run. That is the
property I care about most here, because a capacity leak is invisible from
outside: the cluster just reports less and less free space with nothing running
on it.

---

## 5. Gangs are starved, as predicted, and the gap widens with size

| | submitted | completed | p50 queue | p95 queue | gangs deferred |
|---|---|---|---|---|---|
| no gangs | 3,767 | 3,442 | 3.25s | 5.5s | 0 |
| 30% gangs, 4–8 tasks | 3,767 | 3,434 | 3.5s | 6s | 103 |
| **30% gangs, 8–16 tasks** | 7,419 | 3,582 | **27s** | **55.75s** | 110 |

Small gangs are nearly free: 8 fewer completions and 0.5s of p95. Large gangs
are not — p95 queue latency goes from 5.5s to 55.75s, a **10× increase**.

This is the no-preemption limitation showing up as a number. A large gang
waits for enough capacity to free up naturally rather than evicting smaller
work to make room, so it queues behind a steady stream of tasks that each fit
in gaps it cannot use. `gangs deferred` counts the cycles where a gang was
examined and found infeasible: 110 deferrals is 110 cycles where that gang
held its tenant's queue and placed nothing.

Fixing it needs reservation-with-backfill — hold capacity for the gang while
letting small work use it until the gang can start — which is a different
project and is not here.

---

## 6. Cycle interval trades latency for throughput monotonically

| interval | cycles | completions | CPU util | p50 queue | p95 queue |
|---|---|---|---|---|---|
| 100ms | 1,200 | 3,698 | **97.0%** | 28.5s | 59.6s |
| 250ms | 480 | 3,608 | 94.3% | 30s | 1m2.25s |
| 1s | 120 | 2,951 | 84.1% | 35s | 1m11s |
| 2s | 60 | 2,376 | **75.1%** | 38s | 1m18s |

Clean and monotonic: 20× the cycle rate buys 22 points of utilisation and 56%
more completions. The mechanism is simply that capacity freed by a completion
sits idle until the next cycle notices it, so the mean idle gap is half the
cycle interval.

What this table does *not* measure is the cost of a shorter interval. The
simulator's clock is virtual, so a cycle is free; on a real control plane each
cycle is CPU spent walking queues and recomputing shares. The per-cycle cost is
bounded by `MaxUnitsPerCycle` (512 by default) and the sensible reading is "go
as short as your control plane's CPU allows", with this table saying the
returns are real down to at least 100ms.

---

## A performance bug this file caught

The first version of the interleaved placement loop recomputed each tenant's
remaining demand by summing its queue on **every iteration** — O(queue depth)
per placement, so quadratic per cycle. It passed every unit test, where queues
are a handful of tasks deep.

The simulator built a 36,000-task backlog and the run went from expected-fast
to over ten minutes without finishing. Keeping a running total
(`tenantQueue.remainingTotal`, decremented in `advance()`) plus bounding how
deep a cycle looks (`MaxUnitsPerCycle`) took the full six-group suite to
**about 1 second**.

Worth recording because the bug was invisible at test scale and obvious at
simulation scale, which is most of the argument for having a simulator at all.

---

## Summary

| Claim | Status |
|---|---|
| fair share prevents starvation | **confirmed** — small tenant p95 0s under 12× load asymmetry |
| quota prevents starvation | **refuted** — fair share already did; quota only idled 44% of the cluster |
| weighted shares converge to the ratio | **confirmed** — 74/26 against a predicted 75/25 |
| DRF handles heterogeneous shapes | **confirmed** — opposite-shape tenants within 1% of equal |
| packing bias matters | **refuted** — inert across its full range |
| no capacity leak across failures | **confirmed** — accounting audit clean on every cycle |
| gangs can be starved without preemption | **confirmed** — 10× p95 latency at 8–16 tasks |
| shorter cycles raise utilisation | **confirmed** — monotonic, 75% → 97% from 2s to 100ms |
| worker-removal cost | **not measured** — the simulator bypasses the detection path |
