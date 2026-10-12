package dovetail

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// HTTP surface, split into two audiences with different needs.
//
// Workers call /register, /heartbeat, /task/started and /task/finished. Those
// are high-frequency and must stay cheap; a heartbeat that blocks behind a
// scheduling cycle turns a slow cycle into a fleet-wide false outage.
//
// Submitters and operators call /submit, /tenant, /status and /shares. Those
// are rare and can be as expensive as they need to be.
//
// The one design decision worth calling out: **the scheduling cycle is not
// triggered by requests.** It runs on its own ticker. Running it inline on
// submission seems attractive — lower latency for the submitter — and is
// wrong, because it makes scheduling latency a function of submission rate and
// lets a burst of submissions run a burst of cycles, each seeing a fleet
// snapshot the previous one invalidated.

type Server struct {
	sched   *Scheduler
	log     *slog.Logger
	clock   func() time.Time
	maxBody int64
}

func NewServer(s *Scheduler, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{sched: s, log: logger, clock: time.Now, maxBody: 1 << 20}
}

// SetClock overrides the time source, for tests.
func (srv *Server) SetClock(f func() time.Time) { srv.clock = f }

func (srv *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	// Worker-facing.
	mux.HandleFunc("POST /register", srv.handleRegister)
	mux.HandleFunc("POST /heartbeat", srv.handleHeartbeat)
	mux.HandleFunc("POST /task/started", srv.handleTaskStarted)
	mux.HandleFunc("POST /task/finished", srv.handleTaskFinished)
	// Submitter and operator facing.
	mux.HandleFunc("POST /tenant", srv.handleTenant)
	mux.HandleFunc("POST /submit", srv.handleSubmit)
	mux.HandleFunc("POST /worker/drain", srv.handleDrain)
	mux.HandleFunc("GET /status", srv.handleStatus)
	mux.HandleFunc("GET /shares", srv.handleShares)
	mux.HandleFunc("GET /job", srv.handleJob)
	return mux
}

// ---------------------------------------------------------------------------
// Wire types
//
// Resources cross the wire as separate integer fields rather than a string
// like "4 CPU". Parsing a human-friendly format on a path a worker hits every
// few seconds buys nothing and adds a class of error where a typo becomes a
// capacity claim.
// ---------------------------------------------------------------------------

type ResourcesJSON struct {
	MilliCPU int64 `json:"milli_cpu"`
	MemBytes int64 `json:"mem_bytes"`
}

func (r ResourcesJSON) to() Resources {
	return Resources{MilliCPU: r.MilliCPU, MemBytes: r.MemBytes}
}

func fromResources(r Resources) ResourcesJSON {
	return ResourcesJSON{MilliCPU: r.MilliCPU, MemBytes: r.MemBytes}
}

type RegisterRequest struct {
	WorkerID string        `json:"worker_id"`
	Capacity ResourcesJSON `json:"capacity"`
	Zone     string        `json:"zone"`
	Rack     string        `json:"rack"`
}

type RegisterResponse struct {
	WorkerID string `json:"worker_id"`
	// HeartbeatIntervalMillis tells the worker how often to report. Sent by
	// the control plane rather than configured on the worker so that the
	// timeout and the interval cannot drift apart across a fleet — a worker
	// heartbeating every 30s against a 15s timeout is permanently dead, and
	// that misconfiguration is invisible from either side alone.
	HeartbeatIntervalMillis int64 `json:"heartbeat_interval_millis"`
}

type HeartbeatRequest struct {
	WorkerID string        `json:"worker_id"`
	Free     ResourcesJSON `json:"free"`
	// Running is the worker's own list of what it believes it is executing.
	// Authoritative about execution; the reconciler compares it against the
	// control plane's view of intent.
	Running []string `json:"running"`
}

type HeartbeatResponse struct {
	// Assignments the worker should be running but may not know about.
	// Returned on the heartbeat rather than pushed, so the control plane
	// needs no inbound connectivity to workers — which is what lets workers
	// sit behind NAT without a tunnel.
	Assign []AssignmentJSON `json:"assign"`
	// Cancel lists tasks the worker should stop. Sent when the control plane
	// has given up on a task the worker still reports.
	Cancel []string `json:"cancel"`
	Epoch  uint64   `json:"epoch"`
}

type AssignmentJSON struct {
	TaskID  string        `json:"task_id"`
	Tenant  string        `json:"tenant"`
	Request ResourcesJSON `json:"request"`
	// Epoch lets a worker reject a replayed stale command instead of starting
	// a second copy of a task.
	Epoch uint64 `json:"epoch"`
}

type SubmitRequest struct {
	JobID       string `json:"job_id"`
	Tenant      string `json:"tenant"`
	Priority    int    `json:"priority"`
	Gang        bool   `json:"gang"`
	MaxAttempts int    `json:"max_attempts"`
	Tasks       []struct {
		TaskID  string        `json:"task_id"`
		Request ResourcesJSON `json:"request"`
	} `json:"tasks"`
}

type TenantRequest struct {
	TenantID string        `json:"tenant_id"`
	Quota    ResourcesJSON `json:"quota"`
	Weight   float64       `json:"weight"`
}

type StatusResponse struct {
	Fleet       FleetStatus `json:"fleet"`
	Stats       Stats       `json:"stats"`
	CPUUtil     float64     `json:"cpu_utilization"`
	MemUtil     float64     `json:"mem_utilization"`
	Assignments int         `json:"assignments"`
}

type ShareJSON struct {
	Tenant    string        `json:"tenant"`
	Dominant  float64       `json:"dominant_share"`
	Weighted  float64       `json:"weighted_share"`
	Dimension string        `json:"dominant_dimension"`
	Usage     ResourcesJSON `json:"usage"`
	Demand    ResourcesJSON `json:"demand"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

func (srv *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest
	if !srv.decode(w, r, &req) {
		return
	}
	_, err := srv.sched.registry.Register(req.WorkerID, req.Capacity.to(),
		Topology{Zone: req.Zone, Rack: req.Rack}, srv.clock())
	if err != nil {
		srv.fail(w, http.StatusBadRequest, err)
		return
	}
	srv.log.Info("worker registered", "worker", req.WorkerID,
		"capacity", req.Capacity.to().String(), "zone", req.Zone)

	// A third of the timeout: two consecutive losses still leave a margin
	// before the worker is declared stale. Reporting the interval rather than
	// the timeout keeps the arithmetic in one place.
	interval := srv.sched.config.HeartbeatTimeout / 3
	srv.respond(w, http.StatusOK, RegisterResponse{
		WorkerID:                req.WorkerID,
		HeartbeatIntervalMillis: interval.Milliseconds(),
	})
}

func (srv *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req HeartbeatRequest
	if !srv.decode(w, r, &req) {
		return
	}
	now := srv.clock()

	if err := srv.sched.registry.Heartbeat(req.WorkerID, req.Free.to(), req.Running, now); err != nil {
		// 409 rather than 400: the worker's request is well-formed, the
		// control plane simply does not know it. The status tells the worker
		// to re-register rather than to give up or retry unchanged.
		srv.fail(w, http.StatusConflict, err)
		return
	}

	// Tell the worker about assignments it may not have seen. Pull rather
	// than push, so the control plane needs no route to the worker.
	srv.sched.mu.Lock()
	var assign []AssignmentJSON
	reported := make(map[string]bool, len(req.Running))
	for _, id := range req.Running {
		reported[id] = true
	}
	for _, a := range srv.sched.ledger.AssignmentsOn(req.WorkerID) {
		if !reported[a.TaskID] {
			assign = append(assign, AssignmentJSON{
				TaskID:  a.TaskID,
				Tenant:  a.Tenant,
				Request: fromResources(a.Request),
				Epoch:   a.Epoch,
			})
		}
	}
	// Anything the worker reports that we have no record of should stop. The
	// reconciler adopts what it can first, so by the time this runs an
	// unknown task is genuinely unwanted.
	var cancel []string
	for _, id := range req.Running {
		if _, tracked := srv.sched.ledger.Get(id); !tracked {
			cancel = append(cancel, id)
		}
	}
	epoch := srv.sched.epoch
	srv.sched.mu.Unlock()

	srv.respond(w, http.StatusOK, HeartbeatResponse{Assign: assign, Cancel: cancel, Epoch: epoch})
}

func (srv *Server) handleTaskStarted(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TaskID   string `json:"task_id"`
		WorkerID string `json:"worker_id"`
	}
	if !srv.decode(w, r, &req) {
		return
	}
	if err := srv.sched.TaskStarted(req.TaskID, req.WorkerID, srv.clock()); err != nil {
		srv.fail(w, http.StatusConflict, err)
		return
	}
	srv.respond(w, http.StatusOK, map[string]string{"status": "running"})
}

func (srv *Server) handleTaskFinished(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TaskID  string `json:"task_id"`
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	if !srv.decode(w, r, &req) {
		return
	}
	if err := srv.sched.TaskFinished(req.TaskID, req.Success, req.Error, srv.clock()); err != nil {
		srv.fail(w, http.StatusNotFound, err)
		return
	}
	srv.respond(w, http.StatusOK, map[string]string{"status": "recorded"})
}

func (srv *Server) handleTenant(w http.ResponseWriter, r *http.Request) {
	var req TenantRequest
	if !srv.decode(w, r, &req) {
		return
	}
	err := srv.sched.AddTenant(Tenant{ID: req.TenantID, Quota: req.Quota.to(), Weight: req.Weight})
	if err != nil {
		srv.fail(w, http.StatusBadRequest, err)
		return
	}
	srv.respond(w, http.StatusOK, map[string]string{"tenant": req.TenantID})
}

func (srv *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req SubmitRequest
	if !srv.decode(w, r, &req) {
		return
	}
	job := &Job{
		ID: req.JobID, Tenant: req.Tenant, Priority: req.Priority,
		Gang: req.Gang, MaxAttempts: req.MaxAttempts,
	}
	for _, t := range req.Tasks {
		job.Tasks = append(job.Tasks, &Task{ID: t.TaskID, Request: t.Request.to()})
	}
	if err := srv.sched.Submit(job, srv.clock()); err != nil {
		srv.fail(w, http.StatusBadRequest, err)
		return
	}
	srv.respond(w, http.StatusCreated, map[string]any{"job_id": job.ID, "tasks": len(job.Tasks)})
}

func (srv *Server) handleDrain(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkerID string `json:"worker_id"`
		Draining bool   `json:"draining"`
	}
	if !srv.decode(w, r, &req) {
		return
	}
	if err := srv.sched.registry.SetDraining(req.WorkerID, req.Draining); err != nil {
		srv.fail(w, http.StatusNotFound, err)
		return
	}
	srv.respond(w, http.StatusOK, map[string]any{"worker_id": req.WorkerID, "draining": req.Draining})
}

func (srv *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	now := srv.clock()
	cpu, mem := srv.sched.Utilization(now)
	srv.sched.mu.Lock()
	assignments := srv.sched.ledger.Len()
	srv.sched.mu.Unlock()

	srv.respond(w, http.StatusOK, StatusResponse{
		Fleet:       srv.sched.registry.Status(now, srv.sched.config.HeartbeatTimeout),
		Stats:       srv.sched.Stats(),
		CPUUtil:     cpu,
		MemUtil:     mem,
		Assignments: assignments,
	})
}

func (srv *Server) handleShares(w http.ResponseWriter, _ *http.Request) {
	shares := srv.sched.Shares(srv.clock())
	out := make([]ShareJSON, 0, len(shares))
	for _, s := range shares {
		out = append(out, ShareJSON{
			Tenant: s.Tenant, Dominant: s.Dominant, Weighted: s.Weighted,
			Dimension: s.Dimension, Usage: fromResources(s.Usage), Demand: fromResources(s.Demand),
		})
	}
	srv.respond(w, http.StatusOK, map[string]any{
		"shares":         out,
		"fairness_index": FairnessIndex(shares),
	})
}

func (srv *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		srv.fail(w, http.StatusBadRequest, errors.New("missing id"))
		return
	}
	job, ok := srv.sched.Job(id)
	if !ok {
		srv.fail(w, http.StatusNotFound, fmt.Errorf("unknown job %s", id))
		return
	}
	type taskJSON struct {
		TaskID   string `json:"task_id"`
		Phase    string `json:"phase"`
		WorkerID string `json:"worker_id,omitempty"`
		Attempts int    `json:"attempts"`
		Error    string `json:"error,omitempty"`
	}
	tasks := make([]taskJSON, 0, len(job.Tasks))
	for _, t := range job.Tasks {
		tasks = append(tasks, taskJSON{t.ID, string(t.Phase), t.WorkerID, t.Attempts, t.LastError})
	}
	srv.respond(w, http.StatusOK, map[string]any{
		"job_id": job.ID, "tenant": job.Tenant, "gang": job.Gang,
		"complete": job.Complete(), "tasks": tasks,
	})
}

// ---------------------------------------------------------------------------
// Plumbing
// ---------------------------------------------------------------------------

func (srv *Server) decode(w http.ResponseWriter, r *http.Request, into any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, srv.maxBody)
	dec := json.NewDecoder(r.Body)
	// Unknown fields are rejected. A worker sending a field the control plane
	// does not understand is a version skew, and silently ignoring it means
	// the worker believes it communicated something it did not.
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		srv.fail(w, http.StatusBadRequest, fmt.Errorf("malformed body: %w", err))
		return false
	}
	return true
}

func (srv *Server) respond(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		srv.log.Error("writing response", "error", err)
	}
}

func (srv *Server) fail(w http.ResponseWriter, code int, err error) {
	srv.respond(w, code, ErrorResponse{Error: err.Error()})
}

// Run starts the scheduling and reconciliation loops plus the HTTP listener.
//
// The two loops run at different rates on purpose. Scheduling is latency-
// sensitive and cheap; reconciliation is neither, and running it every cycle
// would spend most of the control plane's time comparing state that has not
// changed.
func (srv *Server) Run(addr string, cycleInterval, reconcileInterval time.Duration, stop <-chan struct{}) error {
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	go func() {
		cycle := time.NewTicker(cycleInterval)
		reconcile := time.NewTicker(reconcileInterval)
		defer cycle.Stop()
		defer reconcile.Stop()
		for {
			select {
			case <-stop:
				_ = httpSrv.Close()
				return
			case <-cycle.C:
				result, err := srv.sched.Cycle(srv.clock())
				if err != nil {
					// An invariant violation means the accounting is wrong,
					// and continuing to schedule against wrong accounting
					// compounds it. Logged at error level rather than
					// swallowed; the cycle itself already refused to commit.
					srv.log.Error("scheduling cycle failed", "epoch", result.Epoch, "error", err)
					continue
				}
				if len(result.Decisions) > 0 || len(result.Lost) > 0 || len(result.Expired) > 0 {
					srv.log.Info("cycle",
						"epoch", result.Epoch,
						"placed", len(result.Decisions),
						"unplaced", len(result.Unplaced),
						"workers_lost", len(result.Lost),
						"reservations_lapsed", len(result.Expired))
				}
			case <-reconcile.C:
				res := srv.sched.Reconcile(srv.clock())
				for _, d := range res.Discrepancies {
					srv.log.Warn("reconciliation discrepancy",
						"kind", d.Kind, "task", d.TaskID, "worker", d.WorkerID, "detail", d.Detail)
				}
				if err := srv.sched.AuditAccounting(); err != nil {
					srv.log.Error("accounting audit failed", "error", err)
				}
			}
		}
	}()

	srv.log.Info("control plane listening", "addr", addr,
		"cycle_interval", cycleInterval, "reconcile_interval", reconcileInterval)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
