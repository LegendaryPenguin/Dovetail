package dovetail

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
)

// These drive the real HTTP surface the Java worker speaks, which is the only
// place the full loop is exercised: register, heartbeat, pull assignments,
// confirm, finish. The Java side has its own tests for its half; this covers
// the contract between them.

func testServer(t *testing.T, cfg Config) (*httptest.Server, *Server, *Scheduler, *time.Time) {
	t.Helper()
	sched, err := NewScheduler(cfg)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	clock := t0
	srv := NewServer(sched, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv.SetClock(func() time.Time { return clock })
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)
	return ts, srv, sched, &clock
}

func post(t *testing.T, url string, body any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func getJSON(t *testing.T, url string, into any) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if into != nil {
		if err := json.Unmarshal(body, into); err != nil {
			t.Fatalf("decode %s: %v (body %s)", url, err, body)
		}
	}
	return resp.StatusCode
}

func TestFullWorkerLifecycleOverHTTP(t *testing.T) {
	ts, _, sched, clock := testServer(t, DefaultConfig())

	// Register.
	code, body := post(t, ts.URL+"/register", RegisterRequest{
		WorkerID: "w1",
		Capacity: ResourcesJSON{MilliCPU: CPU(4), MemBytes: GiB(8)},
		Zone:     "a",
	})
	if code != http.StatusOK {
		t.Fatalf("register: %d %s", code, body)
	}
	var reg RegisterResponse
	if err := json.Unmarshal(body, &reg); err != nil {
		t.Fatalf("decode register: %v", err)
	}
	// The control plane dictates the interval so it cannot drift from the
	// timeout across a fleet.
	if reg.HeartbeatIntervalMillis <= 0 {
		t.Fatalf("heartbeat interval = %d", reg.HeartbeatIntervalMillis)
	}
	if want := DefaultConfig().HeartbeatTimeout.Milliseconds() / 3; reg.HeartbeatIntervalMillis != want {
		t.Errorf("interval = %dms, want %dms (a third of the timeout)", reg.HeartbeatIntervalMillis, want)
	}

	// First heartbeat makes it placeable.
	code, body = post(t, ts.URL+"/heartbeat", HeartbeatRequest{
		WorkerID: "w1",
		Free:     ResourcesJSON{MilliCPU: CPU(4), MemBytes: GiB(8)},
	})
	if code != http.StatusOK {
		t.Fatalf("heartbeat: %d %s", code, body)
	}

	// Tenant and job.
	if code, body = post(t, ts.URL+"/tenant", TenantRequest{
		TenantID: "acme",
		Quota:    ResourcesJSON{MilliCPU: CPU(100), MemBytes: GiB(200)},
		Weight:   1,
	}); code != http.StatusOK {
		t.Fatalf("tenant: %d %s", code, body)
	}

	submit := SubmitRequest{JobID: "j1", Tenant: "acme"}
	submit.Tasks = append(submit.Tasks, struct {
		TaskID  string        `json:"task_id"`
		Request ResourcesJSON `json:"request"`
	}{TaskID: "j1-t0", Request: ResourcesJSON{MilliCPU: CPU(2), MemBytes: GiB(4)}})
	if code, body = post(t, ts.URL+"/submit", submit); code != http.StatusCreated {
		t.Fatalf("submit: %d %s", code, body)
	}

	// Scheduling is on a ticker, not triggered by requests, so drive a cycle.
	if _, err := sched.Cycle(*clock); err != nil {
		t.Fatalf("cycle: %v", err)
	}

	// The worker pulls its assignment from the heartbeat response.
	code, body = post(t, ts.URL+"/heartbeat", HeartbeatRequest{
		WorkerID: "w1",
		Free:     ResourcesJSON{MilliCPU: CPU(4), MemBytes: GiB(8)},
	})
	if code != http.StatusOK {
		t.Fatalf("heartbeat: %d %s", code, body)
	}
	var hb HeartbeatResponse
	if err := json.Unmarshal(body, &hb); err != nil {
		t.Fatalf("decode heartbeat: %v", err)
	}
	if len(hb.Assign) != 1 {
		t.Fatalf("heartbeat returned %d assignments, want 1 (body %s)", len(hb.Assign), body)
	}
	if hb.Assign[0].TaskID != "j1-t0" || hb.Assign[0].Epoch == 0 {
		t.Errorf("assignment = %+v", hb.Assign[0])
	}

	// Confirm, then report the task running in subsequent heartbeats.
	if code, body = post(t, ts.URL+"/task/started", map[string]string{
		"task_id": "j1-t0", "worker_id": "w1",
	}); code != http.StatusOK {
		t.Fatalf("task/started: %d %s", code, body)
	}

	// Now that it is confirmed, the heartbeat should stop re-offering it.
	_, body = post(t, ts.URL+"/heartbeat", HeartbeatRequest{
		WorkerID: "w1",
		Free:     ResourcesJSON{MilliCPU: CPU(2), MemBytes: GiB(4)},
		Running:  []string{"j1-t0"},
	})
	_ = json.Unmarshal(body, &hb)
	if len(hb.Assign) != 0 {
		t.Errorf("a confirmed task was re-offered: %+v", hb.Assign)
	}

	// Finish.
	if code, body = post(t, ts.URL+"/task/finished", map[string]any{
		"task_id": "j1-t0", "success": true, "error": "",
	}); code != http.StatusOK {
		t.Fatalf("task/finished: %d %s", code, body)
	}

	var job struct {
		Complete bool `json:"complete"`
		Tasks    []struct {
			Phase string `json:"phase"`
		} `json:"tasks"`
	}
	if code := getJSON(t, ts.URL+"/job?id=j1", &job); code != http.StatusOK {
		t.Fatalf("job: %d", code)
	}
	if !job.Complete || job.Tasks[0].Phase != string(PhaseSucceeded) {
		t.Errorf("job state = %+v", job)
	}
}

func TestHeartbeatFromUnknownWorkerReturns409(t *testing.T) {
	// 409 rather than 400: the request is well-formed, the control plane
	// simply does not know the worker. The status tells the Java client to
	// re-register rather than retry unchanged, and its NotRegisteredException
	// keys off exactly this.
	ts, _, _, _ := testServer(t, DefaultConfig())
	code, _ := post(t, ts.URL+"/heartbeat", HeartbeatRequest{
		WorkerID: "never-registered",
		Free:     ResourcesJSON{MilliCPU: CPU(1), MemBytes: GiB(1)},
	})
	if code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", code)
	}
}

func TestHeartbeatAsksWorkerToCancelUnknownTasks(t *testing.T) {
	ts, _, _, _ := testServer(t, DefaultConfig())
	cap := ResourcesJSON{MilliCPU: CPU(4), MemBytes: GiB(8)}
	post(t, ts.URL+"/register", RegisterRequest{WorkerID: "w1", Capacity: cap, Zone: "a"})

	_, body := post(t, ts.URL+"/heartbeat", HeartbeatRequest{
		WorkerID: "w1", Free: cap, Running: []string{"ghost-task"},
	})
	var hb HeartbeatResponse
	if err := json.Unmarshal(body, &hb); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(hb.Cancel) != 1 || hb.Cancel[0] != "ghost-task" {
		t.Fatalf("cancel = %+v, want [ghost-task]", hb.Cancel)
	}
}

func TestUnknownFieldsAreRejected(t *testing.T) {
	// A worker sending a field the control plane does not understand is a
	// version skew. Silently ignoring it means the worker believes it
	// communicated something it did not.
	ts, _, _, _ := testServer(t, DefaultConfig())
	resp, err := http.Post(ts.URL+"/register", "application/json",
		bytes.NewReader([]byte(`{"worker_id":"w1","capacity":{"milli_cpu":1000,"mem_bytes":1024},"future_field":true}`)))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestRegisterRejectsUnusableCapacity(t *testing.T) {
	ts, _, _, _ := testServer(t, DefaultConfig())
	for _, c := range []ResourcesJSON{
		{MilliCPU: 0, MemBytes: 0},
		{MilliCPU: -1, MemBytes: GiB(1)},
	} {
		code, _ := post(t, ts.URL+"/register", RegisterRequest{WorkerID: "bad", Capacity: c})
		if code != http.StatusBadRequest {
			t.Errorf("capacity %+v: status %d, want 400", c, code)
		}
	}
}

func TestStatusAndSharesEndpoints(t *testing.T) {
	ts, _, sched, clock := testServer(t, DefaultConfig())
	cap := ResourcesJSON{MilliCPU: CPU(8), MemBytes: GiB(16)}
	post(t, ts.URL+"/register", RegisterRequest{WorkerID: "w1", Capacity: cap, Zone: "a"})
	post(t, ts.URL+"/heartbeat", HeartbeatRequest{WorkerID: "w1", Free: cap})
	post(t, ts.URL+"/tenant", TenantRequest{TenantID: "acme",
		Quota: ResourcesJSON{MilliCPU: CPU(100), MemBytes: GiB(200)}, Weight: 1})

	submit := SubmitRequest{JobID: "j1", Tenant: "acme"}
	for i := 0; i < 3; i++ {
		submit.Tasks = append(submit.Tasks, struct {
			TaskID  string        `json:"task_id"`
			Request ResourcesJSON `json:"request"`
		}{TaskID: fmt.Sprintf("j1-t%d", i), Request: ResourcesJSON{MilliCPU: CPU(2), MemBytes: GiB(4)}})
	}
	post(t, ts.URL+"/submit", submit)
	if _, err := sched.Cycle(*clock); err != nil {
		t.Fatalf("cycle: %v", err)
	}

	var status StatusResponse
	if code := getJSON(t, ts.URL+"/status", &status); code != http.StatusOK {
		t.Fatalf("status: %d", code)
	}
	if status.Fleet.Healthy != 1 {
		t.Errorf("healthy workers = %d, want 1", status.Fleet.Healthy)
	}
	if status.Assignments != 3 {
		t.Errorf("assignments = %d, want 3", status.Assignments)
	}
	if status.CPUUtil <= 0 {
		t.Errorf("cpu utilization = %f, want > 0", status.CPUUtil)
	}

	var shares struct {
		Shares        []ShareJSON `json:"shares"`
		FairnessIndex float64     `json:"fairness_index"`
	}
	if code := getJSON(t, ts.URL+"/shares", &shares); code != http.StatusOK {
		t.Fatalf("shares: %d", code)
	}
	if len(shares.Shares) == 0 {
		t.Fatal("no shares reported")
	}
	found := false
	for _, s := range shares.Shares {
		if s.Tenant == "acme" {
			found = true
			if s.Dominant <= 0 {
				t.Errorf("acme dominant share = %f, want > 0", s.Dominant)
			}
			if s.Dimension != "cpu" && s.Dimension != "mem" {
				t.Errorf("dominant dimension = %q", s.Dimension)
			}
		}
	}
	if !found {
		t.Error("acme missing from shares")
	}
}

func TestDrainEndpoint(t *testing.T) {
	ts, _, _, _ := testServer(t, DefaultConfig())
	cap := ResourcesJSON{MilliCPU: CPU(4), MemBytes: GiB(8)}
	post(t, ts.URL+"/register", RegisterRequest{WorkerID: "w1", Capacity: cap})
	if code, body := post(t, ts.URL+"/worker/drain", map[string]any{
		"worker_id": "w1", "draining": true,
	}); code != http.StatusOK {
		t.Fatalf("drain: %d %s", code, body)
	}
	if code, _ := post(t, ts.URL+"/worker/drain", map[string]any{
		"worker_id": "nope", "draining": true,
	}); code != http.StatusNotFound {
		t.Error("draining an unknown worker should 404")
	}
}

func TestSubmitRejectsBadJobs(t *testing.T) {
	ts, _, _, _ := testServer(t, DefaultConfig())
	cap := ResourcesJSON{MilliCPU: CPU(4), MemBytes: GiB(8)}
	post(t, ts.URL+"/register", RegisterRequest{WorkerID: "w1", Capacity: cap})
	post(t, ts.URL+"/heartbeat", HeartbeatRequest{WorkerID: "w1", Free: cap})
	post(t, ts.URL+"/tenant", TenantRequest{TenantID: "acme",
		Quota: ResourcesJSON{MilliCPU: CPU(100), MemBytes: GiB(200)}, Weight: 1})

	// Unknown tenant.
	s := SubmitRequest{JobID: "bad1", Tenant: "nobody"}
	s.Tasks = append(s.Tasks, struct {
		TaskID  string        `json:"task_id"`
		Request ResourcesJSON `json:"request"`
	}{TaskID: "x", Request: ResourcesJSON{MilliCPU: CPU(1), MemBytes: GiB(1)}})
	if code, _ := post(t, ts.URL+"/submit", s); code != http.StatusBadRequest {
		t.Error("unknown tenant should be rejected")
	}

	// No tasks.
	if code, _ := post(t, ts.URL+"/submit", SubmitRequest{JobID: "bad2", Tenant: "acme"}); code != http.StatusBadRequest {
		t.Error("a job with no tasks should be rejected")
	}

	// Task larger than any worker.
	s = SubmitRequest{JobID: "bad3", Tenant: "acme"}
	s.Tasks = append(s.Tasks, struct {
		TaskID  string        `json:"task_id"`
		Request ResourcesJSON `json:"request"`
	}{TaskID: "huge", Request: ResourcesJSON{MilliCPU: CPU(999), MemBytes: GiB(999)}})
	if code, _ := post(t, ts.URL+"/submit", s); code != http.StatusBadRequest {
		t.Error("a task larger than any worker should be rejected at submission")
	}
}

func TestJobEndpointNotFound(t *testing.T) {
	ts, _, _, _ := testServer(t, DefaultConfig())
	if code := getJSON(t, ts.URL+"/job?id=nope", nil); code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", code)
	}
	if code := getJSON(t, ts.URL+"/job", nil); code != http.StatusBadRequest {
		t.Errorf("missing id: status = %d, want 400", code)
	}
}

func TestWrongMethodRejected(t *testing.T) {
	ts, _, _, _ := testServer(t, DefaultConfig())
	resp, err := http.Get(ts.URL + "/register")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET /register: status %d, want 405", resp.StatusCode)
	}
}
