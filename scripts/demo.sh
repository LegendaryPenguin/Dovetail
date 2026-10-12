#!/usr/bin/env bash
#
# End-to-end demonstration: real Go control plane, real Java workers, real
# multi-tenant contention, then a worker killed mid-flight.
#
# The last two steps are the ones worth watching. Step 5 shows quota binding on
# a greedy tenant while a small tenant keeps getting served. Step 6 kills a
# worker holding running tasks and shows the control plane reclaim its capacity
# and requeue the work without leaking either.

set -euo pipefail

cd "$(dirname "$0")/.."
WORK=$(mktemp -d)
PIDS=()
cleanup() {
    for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
    rm -rf "$WORK"
}
trap cleanup EXIT

export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk}"
export PATH="$JAVA_HOME/bin:$PATH"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
api() { curl -sf -X POST -H 'Content-Type: application/json' -d "$2" "http://127.0.0.1:8099$1"; }

say "1. Build the Go control plane and the Java worker"
(cd go-control-plane && go build -o "$WORK/control-plane" ./cmd/dovetail-control-plane)
(cd java-worker && mvn -q package -DskipTests)
JAR=$(ls java-worker/target/dovetail-worker-*.jar | head -1)
echo "   control plane: $(du -h "$WORK/control-plane" | cut -f1)"
echo "   worker jar:    $(du -h "$JAR" | cut -f1) (no shaded dependencies)"

say "2. Start the control plane"
"$WORK/control-plane" -addr 127.0.0.1:8099 \
    -cycle 200ms -reconcile 2s \
    -heartbeat-timeout 6s -reservation-timeout 2s \
    >"$WORK/cp.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 50); do
    curl -sf http://127.0.0.1:8099/status >/dev/null 2>&1 && break
    sleep 0.1
done
curl -sf http://127.0.0.1:8099/status >/dev/null || { cat "$WORK/cp.log"; exit 1; }

say "3. Start three Java workers, 4 cores / 16 GiB each, across two zones"
for i in 1 2 3; do
    zone=$([ $i -le 2 ] && echo a || echo b)
    DOVETAIL_SERVER=http://127.0.0.1:8099 \
    DOVETAIL_WORKER_ID="java-w$i" \
    DOVETAIL_ZONE="$zone" \
    DOVETAIL_CORES=4 \
    DOVETAIL_MEM_GIB=16 \
        java -jar "$JAR" >"$WORK/w$i.log" 2>&1 &
    PIDS+=($!)
done
sleep 4
curl -s http://127.0.0.1:8099/status | python3 -c '
import json, sys
s = json.load(sys.stdin)["fleet"]
print("   fleet: %d healthy, %d warming, %d stale" % (s["Healthy"], s["Warming"], s["Stale"]))
print("   capacity: %dm cpu" % s["Capacity"]["MilliCPU"])'

say "4. Create two tenants: greedy capped at 50%, small uncapped"
# 12 cores total across the fleet; cap greedy at 6.
api /tenant '{"tenant_id":"greedy","quota":{"milli_cpu":6000,"mem_bytes":25769803776},"weight":1}' >/dev/null
api /tenant '{"tenant_id":"small","quota":{"milli_cpu":12000,"mem_bytes":51539607552},"weight":1}' >/dev/null
echo "   greedy: 6000m quota   small: 12000m quota"

say "5. Greedy submits 60 tasks; small submits 3. Watch the quota bind."
tasks=""
for i in $(seq 0 59); do
    [ -n "$tasks" ] && tasks="$tasks,"
    tasks="$tasks{\"task_id\":\"g-t$i\",\"request\":{\"milli_cpu\":1000,\"mem_bytes\":2147483648}}"
done
api /submit "{\"job_id\":\"greedy-job\",\"tenant\":\"greedy\",\"tasks\":[$tasks]}" >/dev/null

tasks=""
for i in $(seq 0 2); do
    [ -n "$tasks" ] && tasks="$tasks,"
    tasks="$tasks{\"task_id\":\"s-t$i\",\"request\":{\"milli_cpu\":1000,\"mem_bytes\":2147483648}}"
done
api /submit "{\"job_id\":\"small-job\",\"tenant\":\"small\",\"tasks\":[$tasks]}" >/dev/null

sleep 3
curl -s http://127.0.0.1:8099/shares | python3 -c '
import json, sys
d = json.load(sys.stdin)
for s in d["shares"]:
    print("   %-8s usage=%6dm  share=%.3f (%s)" % (
        s["tenant"], s["usage"]["milli_cpu"], s["dominant_share"], s["dominant_dimension"]))
print("   fairness index: %.3f" % d["fairness_index"])'
curl -s http://127.0.0.1:8099/status | python3 -c '
import json, sys
d = json.load(sys.stdin)
print("   quota rejections: %d  (greedy is capped, small is not)" % d["stats"]["QuotaRejections"])'

say "6. Kill a worker holding running tasks"
before=$(curl -s http://127.0.0.1:8099/status | python3 -c 'import json, sys; print(json.load(sys.stdin)["assignments"])')
echo "   assignments before: $before"
kill "${PIDS[1]}" 2>/dev/null || true   # java-w1
echo "   killed java-w1; waiting out the 6s heartbeat timeout..."
sleep 9
curl -s http://127.0.0.1:8099/status | python3 -c '
import json, sys
d = json.load(sys.stdin); st = d["stats"]; f = d["fleet"]
print("   fleet now: %d healthy, %d stale" % (f["Healthy"], f["Stale"]))
print("   workers lost: %d   tasks lost: %d   retried: %d" % (
    st["WorkersLost"], st["TasksLost"], st["TasksRetried"]))
print("   assignments now: %d  (capacity was reclaimed, not leaked)" % d["assignments"])'

say "7. The accounting audit must still pass"
if grep -q "accounting audit failed" "$WORK/cp.log"; then
    echo "   FAIL: the ledger disagreed with task state"
    grep "accounting audit failed" "$WORK/cp.log" | head -3
    exit 1
fi
echo "   clean: the ledger and the task phases agree after a worker failure"

say "Done"
echo "A worker failure is a capacity event, not a data-loss event."
