package k8s

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dovetail "github.com/LegendaryPenguin/dovetail"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

var t0 = time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

// ---------------------------------------------------------------------------
// Quantity parsing — the translation most likely to be quietly wrong
// ---------------------------------------------------------------------------

func TestParseCPU(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"500m", 500, true},
		{"1", 1000, true},
		{"2", 2000, true},
		{"0.5", 500, true},
		// 0.1 cores is 100m exactly in decimal but 99.999...m in binary
		// floating point. Truncating would turn every fractional request into
		// one millicore less than asked for.
		{"0.1", 100, true},
		{"0.001", 1, true},
		{"1500m", 1500, true},
		{"0", 0, true},
		{"", 0, false},
		{"-1", 0, false},
		{"-100m", 0, false},
		{"abc", 0, false},
		{"1x", 0, false},
	}
	for _, tc := range cases {
		got, err := ParseCPU(tc.in)
		if (err == nil) != tc.ok {
			t.Errorf("ParseCPU(%q) err = %v, want ok=%v", tc.in, err, tc.ok)
			continue
		}
		if tc.ok && got != tc.want {
			t.Errorf("ParseCPU(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestParseMemoryDistinguishesBinaryFromDecimal(t *testing.T) {
	// The one that matters. 1M is 1,000,000 and 1Mi is 1,048,576 — a 4.8%
	// difference that compounds with every unit. A parser that conflates them
	// produces a cluster that cannot fit what it was sized for.
	cases := []struct {
		in   string
		want int64
	}{
		{"1Ki", 1024},
		{"1Mi", 1048576},
		{"1Gi", 1073741824},
		{"1k", 1000},
		{"1K", 1000},
		{"1M", 1000000},
		{"1G", 1000000000},
		{"1500Mi", 1500 * 1048576},
		{"128974848", 128974848},
		{"1e9", 1000000000},
		{"0", 0},
	}
	for _, tc := range cases {
		got, err := ParseMemory(tc.in)
		if err != nil {
			t.Errorf("ParseMemory(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseMemory(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}

	// And explicitly: the two are not interchangeable.
	mi, _ := ParseMemory("1Mi")
	m, _ := ParseMemory("1M")
	if mi == m {
		t.Error("1Mi and 1M parsed to the same value")
	}
}

func TestParseMemoryRejectsNonsense(t *testing.T) {
	for _, in := range []string{"", "-1", "-1Gi", "abc", "1Xi", "Gi"} {
		if _, err := ParseMemory(in); err == nil {
			t.Errorf("accepted %q", in)
		}
	}
}

func TestFormatRoundTripsExactly(t *testing.T) {
	// A config that does not survive a round trip is one an operator cannot
	// trust to reflect what they wrote.
	for _, in := range []string{"500m", "2", "1500m"} {
		milli, err := ParseCPU(in)
		if err != nil {
			t.Fatalf("ParseCPU(%q): %v", in, err)
		}
		again, err := ParseCPU(FormatCPU(milli))
		if err != nil || again != milli {
			t.Errorf("cpu %q did not round trip: %d -> %q -> %d", in, milli, FormatCPU(milli), again)
		}
	}
	for _, in := range []string{"1Ki", "1Mi", "4Gi"} {
		bytes, _ := ParseMemory(in)
		again, err := ParseMemory(FormatMemory(bytes))
		if err != nil || again != bytes {
			t.Errorf("memory %q did not round trip via %q", in, FormatMemory(bytes))
		}
	}
	// A value that does not divide exactly stays a plain byte count rather
	// than being rendered lossily.
	if got := FormatMemory(1500); got != "1500" {
		t.Errorf("FormatMemory(1500) = %q, want the exact byte count", got)
	}
}

// ---------------------------------------------------------------------------
// Pod translation
// ---------------------------------------------------------------------------

func pod(name, tenant string, cpu, mem string) Pod {
	p := Pod{
		Metadata: ObjectMeta{Name: name, Namespace: "ns"},
		Spec: PodSpec{Containers: []Container{{
			Name: "main",
			Resources: ResourceRequirements{
				Requests: ResourceList{"cpu": cpu, "memory": mem},
			},
		}}},
	}
	if tenant != "" {
		p.Metadata.Annotations = map[string]string{AnnotationTenant: tenant}
	}
	return p
}

func TestPodResourcesSumsContainers(t *testing.T) {
	p := pod("a", "acme", "500m", "1Gi")
	p.Spec.Containers = append(p.Spec.Containers, Container{
		Name: "sidecar",
		Resources: ResourceRequirements{
			Requests: ResourceList{"cpu": "250m", "memory": "512Mi"},
		},
	})
	got, err := PodResources(p)
	if err != nil {
		t.Fatalf("PodResources: %v", err)
	}
	if got.MilliCPU != 750 {
		t.Errorf("cpu = %d, want 750", got.MilliCPU)
	}
	if want := int64(1073741824 + 536870912); got.MemBytes != want {
		t.Errorf("mem = %d, want %d", got.MemBytes, want)
	}
}

func TestPodResourcesUsesRequestsNotLimits(t *testing.T) {
	// Kubernetes schedules on requests; limits bound runtime usage and
	// reserve nothing. Filtering on limits would refuse to pack a cluster
	// that could comfortably hold the work.
	p := pod("a", "acme", "100m", "128Mi")
	p.Spec.Containers[0].Resources.Limits = ResourceList{"cpu": "4", "memory": "8Gi"}
	got, err := PodResources(p)
	if err != nil {
		t.Fatalf("PodResources: %v", err)
	}
	if got.MilliCPU != 100 {
		t.Errorf("cpu = %d, want the request (100), not the limit", got.MilliCPU)
	}
}

func TestPodWithNoRequestsIsRejected(t *testing.T) {
	// Kubernetes allows it (BestEffort); Dovetail does not, because a zero
	// request is placeable on a full worker unbounded times. Saying so beats
	// silently inventing a request.
	p := Pod{
		Metadata: ObjectMeta{Name: "besteffort", Annotations: map[string]string{AnnotationTenant: "acme"}},
		Spec:     PodSpec{Containers: []Container{{Name: "main"}}},
	}
	if _, err := PodResources(p); err == nil {
		t.Fatal("a pod with no resource requests was accepted")
	}
}

func TestPodWithNoContainersIsRejected(t *testing.T) {
	p := Pod{Metadata: ObjectMeta{Name: "empty"}}
	if _, err := PodResources(p); err == nil {
		t.Fatal("a pod with no containers was accepted")
	}
}

func TestJobFromPodRequiresATenant(t *testing.T) {
	// Defaulting to a shared tenant would let any namespace spend someone
	// else's quota.
	if _, err := JobFromPod(pod("a", "", "100m", "128Mi")); err == nil {
		t.Fatal("a pod with no tenant annotation was accepted")
	}
}

func TestJobFromPodCarriesGangAndPriority(t *testing.T) {
	p := pod("a", "acme", "1", "1Gi")
	p.Metadata.Annotations[AnnotationGang] = "true"
	p.Metadata.Annotations[AnnotationMaxAttempts] = "5"
	priority := int32(7)
	p.Spec.Priority = &priority

	job, err := JobFromPod(p)
	if err != nil {
		t.Fatalf("JobFromPod: %v", err)
	}
	if !job.Gang {
		t.Error("gang annotation was not honoured")
	}
	if job.MaxAttempts != 5 {
		t.Errorf("max attempts = %d, want 5", job.MaxAttempts)
	}
	if job.Priority != 7 {
		t.Errorf("priority = %d, want 7", job.Priority)
	}
	if job.ID != "ns/a" {
		t.Errorf("job id = %q, want ns/a", job.ID)
	}
}

func TestInvalidMaxAttemptsIsRejected(t *testing.T) {
	p := pod("a", "acme", "1", "1Gi")
	for _, v := range []string{"0", "-1", "lots"} {
		p.Metadata.Annotations[AnnotationMaxAttempts] = v
		if _, err := JobFromPod(p); err == nil {
			t.Errorf("accepted max-attempts %q", v)
		}
	}
}

func TestPodKeyDefaultsNamespace(t *testing.T) {
	p := Pod{Metadata: ObjectMeta{Name: "x"}}
	if got := PodKey(p); got != "default/x" {
		t.Errorf("PodKey = %q, want default/x", got)
	}
}

func TestNodeCapacityPrefersAllocatable(t *testing.T) {
	// Capacity includes what the kubelet and system daemons reserve.
	// Scheduling against it overcommits every node by the reservation.
	var n Node
	n.Metadata.Name = "node1"
	n.Status.Capacity = ResourceList{"cpu": "8", "memory": "32Gi"}
	n.Status.Allocatable = ResourceList{"cpu": "7800m", "memory": "30Gi"}

	got, err := NodeCapacity(n)
	if err != nil {
		t.Fatalf("NodeCapacity: %v", err)
	}
	if got.MilliCPU != 7800 {
		t.Errorf("cpu = %d, want allocatable 7800", got.MilliCPU)
	}

	// Falls back to capacity when allocatable is absent.
	n.Status.Allocatable = nil
	got, _ = NodeCapacity(n)
	if got.MilliCPU != 8000 {
		t.Errorf("fallback cpu = %d, want 8000", got.MilliCPU)
	}
}

func TestNodeWithNoResourcesIsRejected(t *testing.T) {
	var n Node
	n.Metadata.Name = "empty"
	if _, err := NodeCapacity(n); err == nil {
		t.Fatal("a node reporting nothing was accepted")
	}
}

func TestPhaseMappingIsExplicitAboutLost(t *testing.T) {
	// Dovetail's Lost has no Kubernetes equivalent: it means the worker
	// stopped answering and the task may or may not still be running.
	// Unknown is exactly right, because it is the one phase that does not
	// claim to know.
	cases := map[dovetail.Phase]string{
		dovetail.PhasePending:   "Pending",
		dovetail.PhaseAssigned:  "Pending",
		dovetail.PhaseRunning:   "Running",
		dovetail.PhaseSucceeded: "Succeeded",
		dovetail.PhaseFailed:    "Failed",
		dovetail.PhaseLost:      "Unknown",
	}
	for phase, want := range cases {
		if got := PhaseForTask(phase); got != want {
			t.Errorf("PhaseForTask(%s) = %q, want %q", phase, got, want)
		}
	}
}

func TestStatusCarriesTheReason(t *testing.T) {
	// kube-scheduler surfaces these in pod events, which is the only place an
	// operator sees why their pod did not schedule.
	st := StatusForTask(dovetail.Task{Phase: dovetail.PhasePending, LastError: "tenant is at quota"})
	if st.Phase != "Pending" || st.Reason != "Unschedulable" {
		t.Errorf("status = %+v", st)
	}
	if st.Message != "tenant is at quota" {
		t.Errorf("message = %q", st.Message)
	}
}

// ---------------------------------------------------------------------------
// Scheduler extender
// ---------------------------------------------------------------------------

func harness(t *testing.T) (*dovetail.Scheduler, *httptest.Server, *Extender) {
	t.Helper()
	sched, err := dovetail.NewScheduler(dovetail.DefaultConfig())
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	if err := sched.AddTenant(dovetail.Tenant{
		ID:     "acme",
		Quota:  dovetail.Resources{MilliCPU: dovetail.CPU(1000), MemBytes: dovetail.GiB(2000)},
		Weight: 1,
	}); err != nil {
		t.Fatalf("AddTenant: %v", err)
	}
	ext := NewExtender(sched, quiet())
	ext.Clock = func() time.Time { return t0 }
	ts := httptest.NewServer(ext.Routes())
	t.Cleanup(ts.Close)
	return sched, ts, ext
}

func addWorker(t *testing.T, sched *dovetail.Scheduler, id string, cpu, gib float64, beat bool) {
	t.Helper()
	cap := dovetail.Resources{MilliCPU: dovetail.CPU(cpu), MemBytes: dovetail.GiB(gib)}
	if _, err := sched.Registry().Register(id, cap, dovetail.Topology{Zone: "a"}, t0); err != nil {
		t.Fatalf("register %s: %v", id, err)
	}
	if beat {
		if err := sched.Registry().Heartbeat(id, cap, nil, t0); err != nil {
			t.Fatalf("heartbeat %s: %v", id, err)
		}
	}
}

func postExtender(t *testing.T, url string, args ExtenderArgs, out any) int {
	t.Helper()
	body, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
	}
	return resp.StatusCode
}

func TestFilterReturnsFeasibleNodes(t *testing.T) {
	sched, ts, _ := harness(t)
	addWorker(t, sched, "big", 8, 32, true)
	addWorker(t, sched, "small", 1, 2, true)

	names := []string{"big", "small"}
	var result FilterResult
	code := postExtender(t, ts.URL+"/filter", ExtenderArgs{
		Pod: pod("p", "acme", "4", "16Gi"), NodeNames: &names,
	}, &result)
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if result.NodeNames == nil || len(*result.NodeNames) != 1 || (*result.NodeNames)[0] != "big" {
		t.Fatalf("feasible = %+v", result.NodeNames)
	}
	// A rejection must carry a reason: it is the only thing an operator sees.
	reason, ok := result.FailedNodes["small"]
	if !ok || reason == "" {
		t.Errorf("no reason given for rejecting 'small': %+v", result.FailedNodes)
	}
}

func TestFilterUsesAvailableNotReportedCapacity(t *testing.T) {
	// The double-booking bug, one layer out. If the extender answered from
	// worker.Reported, kube-scheduler would be told a node fits when this
	// cycle has already promised the gap away.
	sched, ts, _ := harness(t)
	addWorker(t, sched, "w1", 4, 16, true)

	// Place a task that consumes most of the worker, without confirming it.
	job := &dovetail.Job{ID: "j1", Tenant: "acme", Tasks: []*dovetail.Task{{
		ID: "j1-t0", Request: dovetail.Resources{MilliCPU: dovetail.CPU(3), MemBytes: dovetail.GiB(12)},
	}}}
	if err := sched.Submit(job, t0); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := sched.Cycle(t0); err != nil {
		t.Fatalf("cycle: %v", err)
	}

	// The worker still reports its full capacity (no heartbeat since), but
	// only 1 CPU is actually available.
	names := []string{"w1"}
	var result FilterResult
	postExtender(t, ts.URL+"/filter", ExtenderArgs{
		Pod: pod("p", "acme", "2", "4Gi"), NodeNames: &names,
	}, &result)

	if result.NodeNames != nil && len(*result.NodeNames) != 0 {
		t.Fatalf("a 2-CPU pod was admitted to a worker with 1 CPU available: %+v",
			result.NodeNames)
	}
	if _, ok := result.FailedNodes["w1"]; !ok {
		t.Error("no rejection reason recorded")
	}
}

func TestFilterRejectsUnregisteredWarmingDrainingAndStaleNodes(t *testing.T) {
	// Builds its own server below with an advanced clock, so the harness's
	// server is not used here.
	sched, _, _ := harness(t)
	addWorker(t, sched, "healthy", 8, 32, true)
	addWorker(t, sched, "warming", 8, 32, false) // registered, never reported
	addWorker(t, sched, "draining", 8, 32, true)
	if err := sched.Registry().SetDraining("draining", true); err != nil {
		t.Fatalf("drain: %v", err)
	}
	addWorker(t, sched, "stale", 8, 32, true)

	ext := NewExtender(sched, quiet())
	// Advance past the heartbeat timeout so "stale" is stale — but re-beat
	// the healthy ones so only "stale" is affected.
	later := t0.Add(2 * dovetail.DefaultConfig().HeartbeatTimeout)
	cap := dovetail.Resources{MilliCPU: dovetail.CPU(8), MemBytes: dovetail.GiB(32)}
	for _, id := range []string{"healthy", "draining"} {
		if err := sched.Registry().Heartbeat(id, cap, nil, later); err != nil {
			t.Fatalf("heartbeat %s: %v", id, err)
		}
	}
	ext.Clock = func() time.Time { return later }
	ts2 := httptest.NewServer(ext.Routes())
	defer ts2.Close()

	names := []string{"healthy", "warming", "draining", "stale", "ghost"}
	var result FilterResult
	postExtender(t, ts2.URL+"/filter", ExtenderArgs{
		Pod: pod("p", "acme", "1", "1Gi"), NodeNames: &names,
	}, &result)

	if result.NodeNames == nil || len(*result.NodeNames) != 1 ||
		(*result.NodeNames)[0] != "healthy" {
		t.Fatalf("feasible = %+v", result.NodeNames)
	}
	for _, name := range []string{"warming", "draining", "stale", "ghost"} {
		if reason, ok := result.FailedNodes[name]; !ok || reason == "" {
			t.Errorf("%s has no rejection reason", name)
		}
	}
	// A node Kubernetes knows about and Dovetail does not must be refused,
	// not guessed at: Dovetail cannot account for capacity it has no record of.
	if result.FailedNodes["ghost"] == "" {
		t.Error("an unregistered node was not explicitly rejected")
	}
}

func TestFilterAcceptsTheNodesObjectForm(t *testing.T) {
	// The protocol allows both `nodes` and `nodenames`, selected by the
	// extender's own config. Handling only one works against exactly one
	// kube-scheduler setup and fails silently as an empty node list.
	sched, ts, _ := harness(t)
	addWorker(t, sched, "w1", 8, 32, true)

	args := ExtenderArgs{Pod: pod("p", "acme", "1", "1Gi")}
	args.Nodes = &struct {
		Items []Node `json:"items"`
	}{}
	var n Node
	n.Metadata.Name = "w1"
	args.Nodes.Items = []Node{n}

	var result FilterResult
	postExtender(t, ts.URL+"/filter", args, &result)
	if result.NodeNames == nil || len(*result.NodeNames) != 1 {
		t.Fatalf("the nodes-object form was not handled: %+v", result)
	}
}

func TestFilterReportsAPodSpecErrorInTheBodyNotTheStatus(t *testing.T) {
	// kube-scheduler treats a non-200 as the extender being broken and may
	// ignore it entirely; an error in the body is attributed to the pod.
	sched, ts, _ := harness(t)
	addWorker(t, sched, "w1", 8, 32, true)

	names := []string{"w1"}
	bad := Pod{
		Metadata: ObjectMeta{Name: "nores", Annotations: map[string]string{AnnotationTenant: "acme"}},
		Spec:     PodSpec{Containers: []Container{{Name: "c"}}},
	}
	var result FilterResult
	code := postExtender(t, ts.URL+"/filter", ExtenderArgs{Pod: bad, NodeNames: &names}, &result)
	if code != http.StatusOK {
		t.Errorf("status %d, want 200 with an error in the body", code)
	}
	if result.Error == "" {
		t.Error("no error reported for an unschedulable pod spec")
	}
}

func TestFilterIgnoresUnknownFields(t *testing.T) {
	// Unlike everywhere else in Dovetail, unknown fields must NOT be
	// rejected: kube-scheduler sends full Pod and Node objects with hundreds
	// of fields, and this package models only the handful it needs.
	sched, ts, _ := harness(t)
	addWorker(t, sched, "w1", 8, 32, true)

	raw := `{"pod":{"metadata":{"name":"p","namespace":"ns",
	           "annotations":{"dovetail.dev/tenant":"acme"},
	           "ownerReferences":[{"kind":"ReplicaSet"}]},
	         "spec":{"containers":[{"name":"c","image":"x",
	           "resources":{"requests":{"cpu":"1","memory":"1Gi"}},
	           "volumeMounts":[{"name":"v","mountPath":"/x"}]}],
	           "tolerations":[{"key":"k"}],"schedulerName":"dovetail"},
	         "status":{"phase":"Pending","qosClass":"Burstable"}},
	        "nodenames":["w1"]}`
	resp, err := http.Post(ts.URL+"/filter", "application/json", bytes.NewReader([]byte(raw)))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	var result FilterResult
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("a realistic pod with extra fields was rejected: %s", result.Error)
	}
	if result.NodeNames == nil || len(*result.NodeNames) != 1 {
		t.Errorf("feasible = %+v", result.NodeNames)
	}
}

func TestPrioritizeScoresInRange(t *testing.T) {
	sched, ts, _ := harness(t)
	addWorker(t, sched, "a", 8, 32, true)
	addWorker(t, sched, "b", 8, 32, true)

	names := []string{"a", "b"}
	var scores []HostPriority
	postExtender(t, ts.URL+"/prioritize", ExtenderArgs{
		Pod: pod("p", "acme", "1", "1Gi"), NodeNames: &names,
	}, &scores)

	if len(scores) != 2 {
		t.Fatalf("got %d scores", len(scores))
	}
	for _, s := range scores {
		// kube-scheduler expects 0-10; an out-of-range score would be
		// silently truncated by it.
		if s.Score < 0 || s.Score > 10 {
			t.Errorf("node %s score %d is outside 0-10", s.Host, s.Score)
		}
	}
}

func TestPrioritizeSkipsUnknownNodes(t *testing.T) {
	sched, ts, _ := harness(t)
	addWorker(t, sched, "known", 8, 32, true)
	names := []string{"known", "ghost"}
	var scores []HostPriority
	postExtender(t, ts.URL+"/prioritize", ExtenderArgs{
		Pod: pod("p", "acme", "1", "1Gi"), NodeNames: &names,
	}, &scores)
	if len(scores) != 1 || scores[0].Host != "known" {
		t.Errorf("scores = %+v", scores)
	}
}

func TestPrioritizeReturnsNoOpinionOnABadPod(t *testing.T) {
	// Prioritize has no error field; an empty list lets kube-scheduler fall
	// back to its own scoring rather than failing the pod.
	sched, ts, _ := harness(t)
	addWorker(t, sched, "w1", 8, 32, true)
	names := []string{"w1"}
	bad := Pod{Metadata: ObjectMeta{Name: "x"}, Spec: PodSpec{Containers: []Container{{Name: "c"}}}}
	var scores []HostPriority
	code := postExtender(t, ts.URL+"/prioritize", ExtenderArgs{Pod: bad, NodeNames: &names}, &scores)
	if code != http.StatusOK {
		t.Errorf("status %d", code)
	}
	if len(scores) != 0 {
		t.Errorf("expected no opinion, got %+v", scores)
	}
}

func TestHealthz(t *testing.T) {
	_, ts, _ := harness(t)
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Submission
// ---------------------------------------------------------------------------

func TestSubmitPodIsIdempotent(t *testing.T) {
	// A controller re-listing its pods is the normal case, not a fault.
	sched, _, _ := harness(t)
	addWorker(t, sched, "w1", 8, 32, true)

	p := pod("app-1", "acme", "1", "2Gi")
	key, created, err := SubmitPod(sched, p, t0)
	if err != nil || !created {
		t.Fatalf("first submit: created=%v err=%v", created, err)
	}
	if key != "ns/app-1" {
		t.Errorf("key = %q", key)
	}

	key2, created2, err := SubmitPod(sched, p, t0)
	if err != nil {
		t.Fatalf("resubmit errored: %v", err)
	}
	if created2 {
		t.Error("resubmitting an existing pod reported it as created")
	}
	if key2 != key {
		t.Errorf("key changed on resubmit: %q then %q", key, key2)
	}
}

func TestSubmitRejectsAnAlreadyBoundPod(t *testing.T) {
	// Scheduling it again would place a second copy.
	sched, _, _ := harness(t)
	p := pod("bound", "acme", "1", "1Gi")
	p.Spec.NodeName = "some-node"
	if _, _, err := SubmitPod(sched, p, t0); err == nil {
		t.Fatal("an already-bound pod was submitted for scheduling")
	}
}

func TestPodStatusReflectsSchedulingOutcome(t *testing.T) {
	sched, _, _ := harness(t)
	addWorker(t, sched, "w1", 8, 32, true)

	key, _, err := SubmitPod(sched, pod("app-1", "acme", "1", "2Gi"), t0)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	st, worker, ok := PodStatusFor(sched, key)
	if !ok {
		t.Fatal("no status for a submitted pod")
	}
	if st.Phase != "Pending" {
		t.Errorf("phase before scheduling = %q, want Pending", st.Phase)
	}

	if _, err := sched.Cycle(t0); err != nil {
		t.Fatalf("cycle: %v", err)
	}
	st, worker, _ = PodStatusFor(sched, key)
	// Assigned maps to Pending — the pod is placed but not confirmed running.
	if st.Phase != "Pending" {
		t.Errorf("phase after assignment = %q, want Pending", st.Phase)
	}
	if worker != "w1" {
		t.Errorf("worker = %q, want w1", worker)
	}

	if err := sched.TaskStarted(key, "w1", t0); err != nil {
		t.Fatalf("TaskStarted: %v", err)
	}
	st, _, _ = PodStatusFor(sched, key)
	if st.Phase != "Running" {
		t.Errorf("phase after start = %q, want Running", st.Phase)
	}

	if err := sched.TaskFinished(key, true, "", t0); err != nil {
		t.Fatalf("TaskFinished: %v", err)
	}
	st, _, _ = PodStatusFor(sched, key)
	if st.Phase != "Succeeded" {
		t.Errorf("phase after success = %q, want Succeeded", st.Phase)
	}
}

func TestAssignedAndUnscheduledAreBothKubernetesPending(t *testing.T) {
	// A lossy part of the mapping, pinned because it is a trap.
	//
	// Dovetail distinguishes Pending (not placed) from Assigned (placed, not
	// yet confirmed running). Kubernetes has one phase for both: Pending. So
	// the phase alone cannot tell "your pod has not been scheduled" from
	// "your pod is scheduled and starting" — and those need very different
	// responses from whoever is looking.
	//
	// The Reason field is what separates them: Unschedulable on a genuinely
	// pending pod, empty on an assigned one. A caller reading only the phase
	// will misreport one of the two.
	pendingStatus := StatusForTask(dovetail.Task{Phase: dovetail.PhasePending})
	assignedStatus := StatusForTask(dovetail.Task{Phase: dovetail.PhaseAssigned})

	if pendingStatus.Phase != "Pending" || assignedStatus.Phase != "Pending" {
		t.Fatalf("phases differ: %q and %q", pendingStatus.Phase, assignedStatus.Phase)
	}
	if pendingStatus.Reason != "Unschedulable" {
		t.Errorf("a genuinely pending pod has reason %q, want Unschedulable", pendingStatus.Reason)
	}
	if assignedStatus.Reason != "" {
		t.Errorf("an assigned pod has reason %q, want empty", assignedStatus.Reason)
	}
}

func TestQuotaRejectionSurfacesAsAnUnschedulableReason(t *testing.T) {
	sched, err := dovetail.NewScheduler(dovetail.DefaultConfig())
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	// A tenant with room for one pod.
	if err := sched.AddTenant(dovetail.Tenant{
		ID:     "small",
		Quota:  dovetail.Resources{MilliCPU: dovetail.CPU(1), MemBytes: dovetail.GiB(2)},
		Weight: 1,
	}); err != nil {
		t.Fatalf("AddTenant: %v", err)
	}
	addWorker(t, sched, "w1", 16, 64, true)

	var keys []string
	for i := 0; i < 3; i++ {
		key, _, err := SubmitPod(sched, pod(fmt.Sprintf("p%d", i), "small", "1", "2Gi"), t0)
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		keys = append(keys, key)
	}
	if _, err := sched.Cycle(t0); err != nil {
		t.Fatalf("cycle: %v", err)
	}

	// Counted via the Dovetail phase rather than the Kubernetes one, because
	// Assigned and Pending are both Kubernetes-Pending — see the test above.
	// Reading only the Kubernetes phase here reported zero pods placed when
	// one had been.
	placed, unschedulable := 0, 0
	for _, key := range keys {
		task, ok := sched.Task(key)
		if !ok {
			t.Fatalf("no task for %s", key)
		}
		st, _, _ := PodStatusFor(sched, key)
		if st.Phase != "Pending" {
			t.Errorf("pod %s phase = %q, want Pending for both placed and queued", key, st.Phase)
		}
		switch task.Phase {
		case dovetail.PhaseAssigned, dovetail.PhaseRunning:
			placed++
			if st.Reason != "" {
				t.Errorf("placed pod %s carries reason %q", key, st.Reason)
			}
		case dovetail.PhasePending:
			unschedulable++
			if st.Reason != "Unschedulable" {
				t.Errorf("queued pod %s reason = %q, want Unschedulable", key, st.Reason)
			}
		default:
			t.Errorf("pod %s in unexpected phase %s", key, task.Phase)
		}
	}
	if placed != 1 {
		t.Errorf("placed %d pods against a 1-CPU quota, want 1", placed)
	}
	if unschedulable != 2 {
		t.Errorf("unschedulable = %d, want 2", unschedulable)
	}
}
