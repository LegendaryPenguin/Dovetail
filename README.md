# 🪵 Dovetail

*A multi-tenant cluster scheduler where the hard part is not putting work on machines, but deciding what should run when several tenants want more than exists — and keeping the books straight when a machine disappears mid-task.*

A dovetail joint holds without fasteners because the pieces are cut to resist being pulled apart. The name is the point: a scheduler's correctness lives in the fit between what it has promised, what is actually running, and what each machine believes — and those three come apart the moment anything fails.

Dovetail is a scheduler and control plane in **4,083 lines of Go** and **767 lines of Java**, backed by **1,598 lines of tests** — **66 tests** across both languages, clean under `-race`. It implements per-tenant quotas, Dominant Resource Fairness with weights, a three-stage placement pipeline, reservations, heartbeat-based failure detection, reconciliation, gang scheduling, and a workload simulator that drives the real scheduler on a virtual clock.

The thing I would point at: **the simulator refuted two of my own design assumptions.** Quota turned out to protect nothing that fair share was not already protecting, and the packing-bias knob I expected to matter is inert across its entire range. Both are in [`bench/results.md`](bench/results.md) as refuted claims rather than quietly dropped.

```
$ ./scripts/demo.sh
== 3. Start three Java workers, 4 cores / 16 GiB each, across two zones
   fleet: 3 healthy, 0 warming, 0 stale
== 5. Greedy submits 60 tasks; small submits 3. Watch the quota bind.
   small    usage=  3000m  share=0.250 (cpu)
   greedy   usage=  6000m  share=0.500 (cpu)
   quota rejections: 16  (greedy is capped, small is not)
== 6. Kill a worker holding running tasks
   workers lost: 1   tasks lost: 2   retried: 27
   assignments now: 6  (capacity was reclaimed, not leaked)
== 7. The accounting audit must still pass
   clean: the ledger and the task phases agree after a worker failure
```

## ✨ Why I Built It

I wanted to build a scheduler where the interesting problem was contention rather than dispatch. Putting a task on a machine with room is a filter and a sort. Deciding which of four tenants gets the last eight cores, and then keeping the accounting correct when the machine holding their work stops answering, is a different kind of problem — and it is the one every real scheduler is actually solving.

The specific thing I could not have told you beforehand: **what "available capacity" means.** It sounds like one number. It is three, and conflating any two of them produces a scheduler that double-books machines in a way that looks like a worker bug.

## 🔭 What It Does

**Enforces per-tenant quotas** as hard ceilings, so a tenant's maximum footprint is knowable in advance.

**Shares contested capacity by Dominant Resource Fairness**, with weights. A tenant using 60% of cluster CPU and 10% of memory has a dominant share of 60%, and tenants are compared on that — which is what lets a CPU-bound and a memory-bound tenant each get a meaningful half.

**Places in three separate stages:** eligibility (may this run?), filtering (which workers *can*?), scoring (which *should*?). Eligibility and filtering are hard predicates; only scoring expresses a preference.

**Distinguishes reserved from running capacity**, so a scheduling cycle cannot place two tasks into the same gap.

**Detects worker failure by heartbeat** and separately expires assignments a healthy worker never confirmed.

**Reconciles** desired state against what workers report, adopting work it had lost track of and releasing work that has vanished.

**Schedules gangs atomically** — all tasks start or none do.

**Simulates** multi-tenant workloads on a virtual clock to compare policies.

## 🧱 How I Built It

**Week 1 — the objects, and the mistake I was about to make.** Jobs, tasks, tenants, workers, resource vectors, leases. I wrote `Resources` as a fixed two-dimensional vector rather than a map, which looks like a limitation and is a correctness decision: a map makes every arithmetic operation partial, and "what is the sum of `{cpu}` and `{cpu, mem}`" is a question whose wrong answer double-books a machine.

**Week 2 — placement as three stages.** My first version folded filtering into scoring by giving infeasible workers a very low score. It placed tasks on workers that could not run them the moment every score went negative, because *least bad* is still a winner when nothing is good. A filter that returns no candidates cannot do that, and the separation has stayed.

**Week 3 — quotas and fair share.** Both, because they answer different questions. A quota is a ceiling that binds on an idle cluster; a share is an ordering that only matters under contention. Treat the share as a ceiling and the cluster idles while work queues; treat the quota as a preference and one tenant takes everything.

**Week 4 — reservations, and the bug that shaped the project.** Covered below.

**Week 5 — failure handling.** Heartbeats, staleness, and reconciliation. The distinction that took longest to get right: the control plane is the source of truth about *intent*, the worker about *execution*, and reconciliation only works if each is believed about the thing it actually knows.

**Week 6 — gangs and topology.** Gang feasibility is decided for the whole set against a scratch ledger before anything is committed, because a half-placed gang holds capacity and makes no progress, and two of them can hold each other off indefinitely.

**Week 7 — Java workers and the simulator.** The worker is the execution side and deliberately a separate process in a separate language: it forced the control-plane/worker boundary to be a real protocol rather than a function call.

### The three capacities

This is the decision the whole project is arranged around.

My first scheduler read `worker.Reported` — free capacity from the last heartbeat — and placed work into it. Within a single cycle it placed two tasks into the same 4 GiB gap, because the heartbeat that reported the gap predated both decisions. The second task failed to start, was retried, and usually landed in the same place again.

The fix was to name three quantities and never confuse them:

| | meaning |
|---|---|
| **Capacity** | the machine's total, from registration. Static. |
| **Reported** | free capacity per the last heartbeat. Already accounts for everything the worker knows it is running. |
| **Reserved** | capacity committed since — assignments the worker has not yet confirmed. |

```
Available = Reported − Reserved(unconfirmed)
```

The subtlety is *unconfirmed*. Once a heartbeat reports a task running, the worker's own figure accounts for it, and continuing to subtract our reservation double-counts — the capacity appears consumed twice and the worker looks full at half utilisation. So confirmation is precisely the moment accounting responsibility transfers from the control plane to the worker's own report. Both halves of that are tested (`TestNoDoubleBookingWithinOneCycle`, `TestConfirmedAssignmentsAreNotDoubleCounted`), because I got each of them wrong in turn.

### Fair share is a loop, not a sort

The second bug worth recording. I computed the tenant ordering once per cycle and then placed each tenant's work to exhaustion in turn. That reads as correct — lowest share goes first — and is not fair sharing at all: the first tenant fills the entire cluster before the second is considered. The ordering decided who got *everything* rather than who got *more*.

The test that caught it had one tenant submit 100 tasks and another submit 2. The second got nothing.

DRF actually specifies a loop: pick the lowest weighted dominant share, place **one** unit, recompute, pick again. Placing one unit at a time is what lets a tenant's share rise mid-cycle and hand the next slot to someone else. That is `fairplace.go`, and it is why the file exists separately from `placement.go`.

## 🧪 How I Tried to Break It

**An accounting audit that re-derives the books two ways.** `AuditAccounting` compares the ledger against task phases independently: every occupying task must hold exactly one reservation and nothing else may. It runs after every cycle in tests and every reconcile pass in production. This catches the leak class where a task finishes and its reservation is never released — invisible from outside, because the cluster simply reports less and less free capacity with nothing running on it.

**Invariant checks inside the cycle.** A cycle that would leave any worker with negative available capacity returns an error instead of committing. The ledger's two indices must agree exactly; a task in one and not the other is capacity that will never be reclaimed.

**Determinism as a test.** Go randomises map iteration, so eight identical runs must produce byte-identical decision sequences. Without this every simulator comparison is noise and every bug reproduction stops reproducing.

**Concurrency on the Java side.** `TaskRegistry` is `synchronized` rather than built on concurrent collections, because the operation that matters is compound: "is this task known, and if not reserve capacity and record it" must be atomic or two assignments for the same task both succeed. A test fires 16 threads at a 10-CPU worker and asserts exactly 10 acceptances.

**Failure paths driven on a virtual clock.** Nothing in the scheduler calls `time.Now()`; every test passes time explicitly. Heartbeat timeouts, reservation expiry and staleness are therefore deterministic rather than sleep-dependent.

### Bugs the tests and simulator found

**Fair share was a sort, not a loop.** Above. Found by a unit test with asymmetric load.

**A quadratic placement loop, invisible at test scale.** `remaining()` summed each tenant's queue on every loop iteration — O(depth) per placement. Unit tests have queues a handful deep and it was free. The simulator built a 36,000-task backlog and the suite went from a second to over ten minutes. A running total plus a per-cycle queue-depth bound fixed it.

**Warming workers were reaped as failures.** My registry had healthy and unhealthy, so a worker was declared stale during the gap between registering and its first heartbeat — its (nonexistent) assignments returned and a failure counted. Cold starts looked like crashes and the fleet appeared to flap. `Warming` is a third state because of that.

**Reconciliation nearly raced the reservation timeout.** An unconfirmed assignment looks exactly like a ghost: the worker does not report it. Treating it as one releases capacity moments before the worker reports the task running. Reconcile now skips unconfirmed reservations and lets the (shorter) reservation timeout own that case.

## 📊 What the Measurements Say

Full detail in [`bench/results.md`](bench/results.md). The headlines:

**Fair share prevents starvation; quota does not add to that.** Under a 12× load asymmetry, the small tenant's p95 queue latency is **0s whether or not the greedy tenant is capped** — identical completion counts too. What capping greedy at 50% *did* achieve was dropping cluster utilisation from 92.5% to 48.6%. A tenant with zero usage has a dominant share of zero and sorts first in every cycle, so it is served on arrival regardless of the backlog ahead of it. Quota is the right tool for predictable capacity planning and the wrong one for starvation.

**Weighted shares converge where DRF says.** 3:1 weights produce 74%/26% of completions against a predicted 75%/25%, and the weight buys latency too (p95 26.75s against 1m26s).

**DRF earns its complexity on heterogeneous shapes.** One tenant asking 4 CPU / 2 GiB per task and another asking 1 CPU / 16 GiB land within 1% of equal completions. Comparing on either resource alone would have badly favoured one. It costs utilisation — 59.5% against 94% for homogeneous work — because opposite shapes fragment the cluster.

**Packing bias is inert.** 76.0% CPU utilisation at every setting from pure-spread to pure-pack, identical to three significant figures. With homogeneous workers and tasks much smaller than a worker, nearly every feasible worker is equally good. A negative result, reported as one.

**Worker failure costs about a second and leaks nothing.** A stalled worker — still registered, silently doing nothing — takes p95 queue latency from 500ms to 1.25s. The accounting audit passes on every cycle of every run.

**Large gangs get starved, by exactly the mechanism predicted.** p95 queue latency goes from 5.5s with no gangs to 55.75s with 8–16-task gangs, a 10× increase, because there is no preemption and a gang waits for natural completions.

## 📚 What I Learned

**"Available capacity" is three numbers wearing one name.** I would now treat any scheduler that has a single notion of free capacity as having a latent double-booking bug. The three-way split is not defensive; it is what the problem actually is.

**Fairness is an ordering you re-derive, not one you compute.** The sort-once version was wrong in a way that read as correct, which is the most dangerous kind of wrong. The general lesson I took: when a policy is about proportions, any implementation that makes a decision once per batch instead of once per allocation is probably not implementing the policy.

**A simulator's job is to refute you.** Two of its six experiments contradicted my design beliefs, and a third found a quadratic bug that unit tests could not see. If a simulator only ever confirms that the system works, it is a demo.

**A quota and a share are not the same shape of thing, and I had to measure it to believe it.** I added quotas to fix starvation. They do not fix starvation; fair share does. Quotas fix a different problem I had not separated out — predictability — and on a cluster with no competing demand they are pure waste.

**The right source of truth depends on the question.** "The control plane is authoritative" is almost right and produces a reconciler that deletes real work. It is authoritative about intent. The worker is authoritative about execution. Reconciliation only works once those are separated.

## ⚖️ What I Do Not Claim

- **The control plane is a single point of failure with no persistence.** State is in memory. A restart loses every job, every tenant and the whole ledger; workers re-register and their running tasks become unadoptable orphans. No leader election, no replication, no write-ahead log. This is the largest gap by a wide margin.
- **Duplicate execution is possible and there is no fencing.** A task on an unreachable-but-alive worker is marked `Lost` and requeued, and it may still be running. The right fix is a fencing token the worker presents to any side effect; `Assignment.Epoch` is the beginning of that and is not enforced anywhere outside the worker's own replay check. If reconciliation finds one task on two workers it reports it and leaves it for a human.
- **No preemption.** A large gang waits for natural completions and can be starved indefinitely by a stream of small tasks — measured at 10× p95 latency. Reservation-with-backfill is the fix and is absent.
- **Gang placement is first-fit-decreasing, not optimal.** Bin packing is NP-hard, so a genuinely optimal gang placement is not on the table; largest-first avoids the specific failure where a gang fragments the cluster and then cannot fit its own largest member.
- **Resources are two dimensions.** No GPUs, no disk, no network bandwidth, no custom resources. Adding a third is mechanical; adding arbitrary ones means giving up total arithmetic, which is what the fixed vector buys.
- **No authentication on any endpoint.** Any process that can reach the port can register as a worker of arbitrary capacity, submit jobs as any tenant, or drain a machine.
- **The Kubernetes adapter is not here.** It was in my original plan for week 7 and I built the simulator instead, because the simulator answers questions and an adapter only moves data. Placement is structured to make one straightforward — `Topology` already carries zone and rack — but it does not exist and the README should not imply otherwise.
- **`MaxUnitsPerCycle` changes behaviour under extreme backlog.** A tenant with 36,000 queued tasks is considered for its first 512 per cycle, so the unplaced reasons a cycle reports cover a prefix of the queue rather than all of it. Correct for throughput, slightly misleading in operator output.
- **The simulator cannot produce genuine control-plane/worker divergence.** It generates heartbeats from the control plane's own view, so a worker only disagrees through explicitly injected failures. The orphan-adoption path is covered by unit tests and under-exercised by simulation.
- **Worker-removal cost is not measured.** The simulator removes a worker from the registry directly, bypassing the staleness reaper, so that scenario measures capacity loss and not detection latency. The stalled-worker scenario is the one with a real number.
- **Priority is within-tenant only.** Across tenants, fair share decides — which is deliberate, since otherwise a tenant buys the cluster by marking everything urgent — but it means there is no true cluster-wide priority for genuinely urgent work.
- **Timestamps come from the control plane's wall clock** with no monotonicity enforcement, and the fair-share computation has no decay, so a tenant's historical usage does not fade. Two tenants with identical current usage are treated identically regardless of who has been hammering the cluster for an hour.
- **No autoscaling.** The fleet is whatever registers.

## 🛠️ Tech Stack

- **Go 1.24**, standard library only — `go.mod` has no `require` block.
- **Java 21**, JUnit 5 test-scoped only. No Jackson, no OkHttp, no logging framework: `java.net.http` plus `java.util.logging` cover six endpoints of flat JSON, and the worker stays a 24 KB jar with no shaded dependencies. The cost is hand-rolled JSON reading, which is narrow by design and tested directly.
- Virtual clocks throughout. Nothing in the scheduler calls `time.Now()`.

## 🚀 Setup

```bash
# Go control plane
cd go-control-plane
go build ./... && go test -race ./...

# Java worker
cd ../java-worker
mvn test && mvn package

# Everything: control plane + 3 real Java workers + a worker killed mid-flight
cd .. && ./scripts/demo.sh

# The six simulation experiments
cd go-control-plane && go run ./cmd/dovetail-sim
go run ./cmd/dovetail-sim -group quota     # or fairness, packing, failure, gangs, cycle
```

Running it by hand:

```bash
go run ./cmd/dovetail-control-plane -addr 127.0.0.1:8080 \
    -cycle 250ms -heartbeat-timeout 15s -reservation-timeout 5s

DOVETAIL_SERVER=http://127.0.0.1:8080 DOVETAIL_WORKER_ID=w1 \
DOVETAIL_CORES=8 DOVETAIL_MEM_GIB=32 \
    java -jar java-worker/target/dovetail-worker-0.1.0.jar

curl -s localhost:8080/status | python3 -m json.tool
curl -s localhost:8080/shares | python3 -m json.tool
```

## 🗺️ Repository Map

```
go-control-plane/
  resources.go      two-dimensional resource vectors; DRF dominant share
  types.go          jobs, tasks, tenants, workers, the three capacities
  reservations.go   the ledger: reserved vs confirmed, and why they differ
  registry.go       worker registration, heartbeats, warming/healthy/stale
  fairshare.go      quota (a ceiling) vs fair share (an ordering)
  placement.go      eligibility / filter / score, kept separate
  fairplace.go      the interleaved DRF placement loop
  gang.go           all-or-nothing placement against a scratch ledger
  reconcile.go      intent vs execution; orphans and ghosts
  scheduler.go      the five-step cycle
  server.go         HTTP: worker-facing and operator-facing
  simulator/        synthetic tenants on a virtual clock
  cmd/              dovetail-control-plane, dovetail-sim

java-worker/
  Resources.java        immutable vector, matching the Go side
  TaskRegistry.java     the worker's authoritative view of its own execution
  ControlPlaneClient.java  HTTP + narrow hand-rolled JSON
  Worker.java           register, heartbeat, run, report

bench/results.md      six experiments, two refuted assumptions
scripts/demo.sh       Go + Java end to end, including a worker killed mid-task
```

## 📖 References

- Ghodsi et al., [*Dominant Resource Fairness*](https://cs.stanford.edu/~matei/papers/2011/nsdi_drf.pdf) (NSDI 2011) — the fairness algorithm, and the reason shares are compared on the dominant dimension.
- Verma et al., [*Large-scale cluster management at Google with Borg*](https://research.google/pubs/pub43438/) (EuroSys 2015) — where the reconciliation framing comes from.
- Schwarzkopf et al., [*Omega*](https://research.google/pubs/pub41684/) (EuroSys 2013) — on optimistic concurrency between scheduling cycles, which Dovetail does not implement.
- Jain et al., *A Quantitative Measure of Fairness and Discrimination* (1984) — the fairness index, and its limits.

## 📄 License

MIT. See [LICENSE](LICENSE).
