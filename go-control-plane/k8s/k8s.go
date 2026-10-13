// Package k8s translates between Kubernetes Pod specs and Dovetail jobs.
//
// **What this is, stated precisely**, because "Kubernetes integration" covers
// a range from a type translation to a working scheduler extender:
//
//   - It translates Pod specs into Dovetail jobs and Dovetail task state back
//     into Pod status, including resource-request parsing for the quantity
//     formats Kubernetes actually uses (`500m`, `2`, `1Gi`, `1500Mi`).
//   - It implements the Kubernetes **scheduler extender** HTTP protocol —
//     `/filter` and `/prioritize` — so a real kube-scheduler can delegate
//     node selection to Dovetail's filter and score stages.
//   - It does **not** watch the API server, bind pods, or run as a kubelet.
//     There is no client-go dependency and no cluster connection. Feed it
//     Pod JSON and it answers; it does not go looking.
//
// That boundary is deliberate rather than a shortcut. The interesting part of
// Dovetail is the scheduling decision, and the extender protocol is exactly
// the seam where a real cluster asks for one. Everything on the far side of
// that seam — informers, leader election, the bind call, resource versions —
// is Kubernetes plumbing that would triple the code and test nothing about
// scheduling.
//
// The translation that matters most is the resource one. Kubernetes quantities
// are a small language with suffixes that mean different things (`m` is
// milli-, `Mi` is mebi-, `M` is mega-), and a parser that treats `1M` and
// `1Mi` as the same number is off by 4.8% — which is the kind of error that
// shows up as a cluster that mysteriously cannot fit what it was sized for.
package k8s

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	dovetail "github.com/LegendaryPenguin/dovetail"
)

// ---------------------------------------------------------------------------
// Resource quantities
// ---------------------------------------------------------------------------

// ParseCPU converts a Kubernetes CPU quantity to millicores.
//
// Accepts "500m" (millicores), "2" (cores), "0.5" (fractional cores). The
// fractional form is legal and common in hand-written YAML even though the
// canonical form is milli.
func ParseCPU(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty cpu quantity")
	}
	if strings.HasSuffix(s, "m") {
		v, err := strconv.ParseInt(strings.TrimSuffix(s, "m"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cpu %q: %w", s, err)
		}
		if v < 0 {
			return 0, fmt.Errorf("cpu %q is negative", s)
		}
		return v, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("cpu %q: %w", s, err)
	}
	if f < 0 {
		return 0, fmt.Errorf("cpu %q is negative", s)
	}
	// Round rather than truncate. "0.1" cores is 100m exactly in decimal but
	// 99.999...m in binary floating point, and truncation would turn every
	// fractional request into one millicore less than asked for.
	return int64(math.Round(f * 1000)), nil
}

// ParseMemory converts a Kubernetes memory quantity to bytes.
//
// The suffix table is the part worth getting right. Kubernetes uses both
// binary (Ki, Mi, Gi — powers of 1024) and decimal (K, M, G — powers of 1000)
// suffixes, and they are not interchangeable: 1M is 1,000,000 and 1Mi is
// 1,048,576, a 4.8% difference. A parser that conflates them produces a
// cluster that cannot fit what it was sized for, and the error compounds with
// every unit.
func ParseMemory(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty memory quantity")
	}

	// Longest suffixes first, so "Mi" is matched before "M".
	suffixes := []struct {
		suffix     string
		multiplier int64
	}{
		{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40},
		{"Pi", 1 << 50}, {"Ei", 1 << 60},
		{"k", 1000}, {"K", 1000}, {"M", 1000_000}, {"G", 1000_000_000},
		{"T", 1000_000_000_000}, {"P", 1000_000_000_000_000},
	}
	for _, s2 := range suffixes {
		if strings.HasSuffix(s, s2.suffix) {
			digits := strings.TrimSuffix(s, s2.suffix)
			f, err := strconv.ParseFloat(digits, 64)
			if err != nil {
				return 0, fmt.Errorf("memory %q: %w", s, err)
			}
			if f < 0 {
				return 0, fmt.Errorf("memory %q is negative", s)
			}
			return int64(f * float64(s2.multiplier)), nil
		}
	}

	// Exponent notation: "1e9".
	if strings.ContainsAny(s, "eE") {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("memory %q: %w", s, err)
		}
		if f < 0 {
			return 0, fmt.Errorf("memory %q is negative", s)
		}
		return int64(f), nil
	}

	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("memory %q: %w", s, err)
	}
	if v < 0 {
		return 0, fmt.Errorf("memory %q is negative", s)
	}
	return v, nil
}

// FormatCPU renders millicores the way Kubernetes would.
func FormatCPU(milli int64) string {
	if milli%1000 == 0 && milli != 0 {
		return strconv.FormatInt(milli/1000, 10)
	}
	return strconv.FormatInt(milli, 10) + "m"
}

// FormatMemory renders bytes using a binary suffix when it divides exactly.
//
// Exact division only: rendering 1500 bytes as "1.46Ki" would be a lossy
// round trip, and a config that does not survive a round trip is one an
// operator cannot trust to reflect what they wrote.
func FormatMemory(bytes int64) string {
	units := []struct {
		suffix string
		size   int64
	}{{"Gi", 1 << 30}, {"Mi", 1 << 20}, {"Ki", 1 << 10}}
	for _, u := range units {
		if bytes >= u.size && bytes%u.size == 0 {
			return strconv.FormatInt(bytes/u.size, 10) + u.suffix
		}
	}
	return strconv.FormatInt(bytes, 10)
}

// ---------------------------------------------------------------------------
// Pod types
//
// Only the fields the scheduler needs. A full PodSpec is hundreds of fields
// and reproducing it would be transcription, not engineering.
// ---------------------------------------------------------------------------

type ResourceList map[string]string

type ResourceRequirements struct {
	Requests ResourceList `json:"requests,omitempty"`
	Limits   ResourceList `json:"limits,omitempty"`
}

type Container struct {
	Name      string               `json:"name"`
	Image     string               `json:"image,omitempty"`
	Resources ResourceRequirements `json:"resources,omitempty"`
}

type ObjectMeta struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace,omitempty"`
	UID         string            `json:"uid,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type PodSpec struct {
	Containers []Container `json:"containers"`
	// NodeName, when set, means the pod is already bound and the scheduler
	// should not be asked about it.
	NodeName          string `json:"nodeName,omitempty"`
	PriorityClassName string `json:"priorityClassName,omitempty"`
	Priority          *int32 `json:"priority,omitempty"`
}

type PodStatus struct {
	Phase   string `json:"phase,omitempty"`
	Message string `json:"message,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

type Pod struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     PodSpec    `json:"spec"`
	Status   PodStatus  `json:"status,omitempty"`
}

type Node struct {
	Metadata ObjectMeta `json:"metadata"`
	Status   struct {
		Capacity    ResourceList `json:"capacity,omitempty"`
		Allocatable ResourceList `json:"allocatable,omitempty"`
	} `json:"status,omitempty"`
}

// ---------------------------------------------------------------------------
// Translation
// ---------------------------------------------------------------------------

// Annotation keys Dovetail reads off a Pod.
const (
	// AnnotationTenant names the Dovetail tenant. Required: a pod with no
	// tenant cannot be quota-accounted, and defaulting it to a shared tenant
	// would let any namespace spend someone else's quota.
	AnnotationTenant = "dovetail.dev/tenant"
	// AnnotationGang marks the pod as part of an all-or-nothing group.
	AnnotationGang = "dovetail.dev/gang"
	// AnnotationGangSize is how many pods the gang expects.
	AnnotationGangSize = "dovetail.dev/gang-size"
	// AnnotationMaxAttempts overrides the retry bound.
	AnnotationMaxAttempts = "dovetail.dev/max-attempts"
)

// PodResources sums the requests across a pod's containers.
//
// Requests, not limits. Kubernetes schedules on requests — limits bound
// runtime usage and do not reserve anything — and a scheduler that filtered on
// limits would refuse to pack a cluster that could comfortably hold the work.
//
// A container with no requests contributes nothing, which matches Kubernetes
// (a request-less pod is BestEffort and schedulable anywhere). That is worth
// flagging rather than silently allowing, because Dovetail's own Submit path
// rejects a zero request — so the conversion reports it instead.
func PodResources(pod Pod) (dovetail.Resources, error) {
	var total dovetail.Resources
	if len(pod.Spec.Containers) == 0 {
		return total, fmt.Errorf("pod %s has no containers", pod.Metadata.Name)
	}
	for _, c := range pod.Spec.Containers {
		if cpu, ok := c.Resources.Requests["cpu"]; ok {
			milli, err := ParseCPU(cpu)
			if err != nil {
				return total, fmt.Errorf("pod %s container %s: %w", pod.Metadata.Name, c.Name, err)
			}
			total.MilliCPU += milli
		}
		if mem, ok := c.Resources.Requests["memory"]; ok {
			bytes, err := ParseMemory(mem)
			if err != nil {
				return total, fmt.Errorf("pod %s container %s: %w", pod.Metadata.Name, c.Name, err)
			}
			total.MemBytes += bytes
		}
	}
	if total.IsZero() {
		// Dovetail requires a non-empty request: a zero request is placeable
		// on a full worker unbounded times, which is a category error for a
		// capacity scheduler. Kubernetes allows it (BestEffort); Dovetail does
		// not, and saying so beats silently inventing a request.
		return total, fmt.Errorf("pod %s requests no resources; Dovetail requires an explicit request",
			pod.Metadata.Name)
	}
	return total, nil
}

// NodeCapacity reads a node's allocatable resources, falling back to capacity.
//
// Allocatable first, because capacity includes what the kubelet and system
// daemons reserve. Scheduling against capacity overcommits every node by the
// reservation, which is the sort of error that only shows up under load.
func NodeCapacity(node Node) (dovetail.Resources, error) {
	var out dovetail.Resources
	source := node.Status.Allocatable
	if len(source) == 0 {
		source = node.Status.Capacity
	}
	if len(source) == 0 {
		return out, fmt.Errorf("node %s reports neither allocatable nor capacity",
			node.Metadata.Name)
	}
	if cpu, ok := source["cpu"]; ok {
		milli, err := ParseCPU(cpu)
		if err != nil {
			return out, fmt.Errorf("node %s: %w", node.Metadata.Name, err)
		}
		out.MilliCPU = milli
	}
	if mem, ok := source["memory"]; ok {
		bytes, err := ParseMemory(mem)
		if err != nil {
			return out, fmt.Errorf("node %s: %w", node.Metadata.Name, err)
		}
		out.MemBytes = bytes
	}
	if out.IsZero() {
		return out, fmt.Errorf("node %s reports no cpu or memory", node.Metadata.Name)
	}
	return out, nil
}

// JobFromPod converts a single Pod into a one-task Dovetail job.
func JobFromPod(pod Pod) (*dovetail.Job, error) {
	tenant := pod.Metadata.Annotations[AnnotationTenant]
	if tenant == "" {
		// Refused rather than defaulted. A pod with no tenant cannot be
		// quota-accounted, and defaulting to a shared tenant would let any
		// namespace spend someone else's quota.
		return nil, fmt.Errorf("pod %s has no %s annotation", pod.Metadata.Name, AnnotationTenant)
	}

	request, err := PodResources(pod)
	if err != nil {
		return nil, err
	}

	id := PodKey(pod)
	job := &dovetail.Job{
		ID:     id,
		Tenant: tenant,
		Gang:   pod.Metadata.Annotations[AnnotationGang] == "true",
	}
	if v := pod.Metadata.Annotations[AnnotationMaxAttempts]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("pod %s has an invalid %s: %q",
				pod.Metadata.Name, AnnotationMaxAttempts, v)
		}
		job.MaxAttempts = n
	}
	if pod.Spec.Priority != nil {
		job.Priority = int(*pod.Spec.Priority)
	}
	job.Tasks = []*dovetail.Task{{ID: id, Request: request}}
	return job, nil
}

// PodKey is the stable identifier Dovetail uses for a pod.
//
// namespace/name rather than the UID. The UID is more correct in principle —
// it survives a name being reused — but it is opaque, so every log line and
// every error message becomes unreadable. Namespace/name is what an operator
// types, and a recreated pod with the same name is a new scheduling decision
// anyway.
func PodKey(pod Pod) string {
	ns := pod.Metadata.Namespace
	if ns == "" {
		ns = "default"
	}
	return ns + "/" + pod.Metadata.Name
}

// PhaseForTask maps a Dovetail phase onto a Kubernetes pod phase.
//
// The lossy part, and worth naming: Dovetail's Assigned and Running both map
// to Kubernetes' Pending and Running respectively, but Dovetail's **Lost** has
// no Kubernetes equivalent. Lost means "the worker stopped answering and the
// task may or may not still be running", which Kubernetes expresses as
// Unknown — and Unknown is exactly right, because it is the one phase that
// does not claim to know.
func PhaseForTask(phase dovetail.Phase) string {
	switch phase {
	case dovetail.PhasePending, dovetail.PhaseAssigned:
		return "Pending"
	case dovetail.PhaseRunning:
		return "Running"
	case dovetail.PhaseSucceeded:
		return "Succeeded"
	case dovetail.PhaseFailed:
		return "Failed"
	case dovetail.PhaseLost:
		return "Unknown"
	}
	return "Unknown"
}

// StatusForTask builds a PodStatus from Dovetail task state.
func StatusForTask(task dovetail.Task) PodStatus {
	status := PodStatus{Phase: PhaseForTask(task.Phase)}
	if task.LastError != "" {
		status.Message = task.LastError
	}
	switch task.Phase {
	case dovetail.PhasePending:
		status.Reason = "Unschedulable"
	case dovetail.PhaseLost:
		status.Reason = "NodeLost"
	case dovetail.PhaseFailed:
		status.Reason = "Failed"
	}
	return status
}

// ---------------------------------------------------------------------------
// Scheduler extender
//
// The Kubernetes scheduler-extender HTTP protocol. kube-scheduler calls
// /filter with a pod and a node list and expects the feasible subset, then
// /prioritize and expects a score per node. This is the seam where a real
// cluster asks Dovetail for a decision, which is why it is the integration
// point worth building rather than an informer loop.
// ---------------------------------------------------------------------------

type ExtenderArgs struct {
	Pod   Pod `json:"pod"`
	Nodes *struct {
		Items []Node `json:"items"`
	} `json:"nodes,omitempty"`
	NodeNames *[]string `json:"nodenames,omitempty"`
}

type FilterResult struct {
	Nodes *struct {
		Items []Node `json:"items"`
	} `json:"nodes,omitempty"`
	NodeNames *[]string `json:"nodenames,omitempty"`
	// FailedNodes maps a rejected node to the reason. kube-scheduler surfaces
	// these in the pod's events, which is the only place an operator sees why
	// their pod did not schedule — so an empty reason here is a support ticket.
	FailedNodes map[string]string `json:"failedNodes,omitempty"`
	Error       string            `json:"error,omitempty"`
}

type HostPriority struct {
	Host  string `json:"host"`
	Score int64  `json:"score"`
}

// Extender answers filter and prioritize calls using Dovetail's own stages.
type Extender struct {
	sched *dovetail.Scheduler
	log   *slog.Logger
	// Clock is injected so the extender's view of worker health is testable
	// without sleeping.
	Clock func() time.Time
}

func NewExtender(sched *dovetail.Scheduler, logger *slog.Logger) *Extender {
	if logger == nil {
		logger = slog.Default()
	}
	return &Extender{sched: sched, log: logger, Clock: time.Now}
}

func (e *Extender) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /filter", e.handleFilter)
	mux.HandleFunc("POST /prioritize", e.handlePrioritize)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

func (e *Extender) handleFilter(w http.ResponseWriter, r *http.Request) {
	args, ok := e.decode(w, r)
	if !ok {
		return
	}

	request, err := PodResources(args.Pod)
	if err != nil {
		// An unschedulable pod spec is reported through the extender's error
		// field rather than as an HTTP error: kube-scheduler treats a non-200
		// as the extender being broken and may ignore it entirely, whereas an
		// error in the body is attributed to the pod.
		e.respond(w, http.StatusOK, FilterResult{Error: err.Error()})
		return
	}

	names := nodeNames(args)
	if len(names) == 0 {
		e.respond(w, http.StatusOK, FilterResult{Error: "no nodes supplied"})
		return
	}

	feasible := make([]string, 0, len(names))
	failed := map[string]string{}
	now := e.Clock()

	for _, name := range names {
		worker, known := e.sched.Registry().Get(name)
		if !known {
			// A node Kubernetes knows about and Dovetail does not. Refusing is
			// correct: Dovetail cannot account for capacity it has no record
			// of, and guessing would corrupt the accounting that the whole
			// reservation ledger exists to protect.
			failed[name] = "not registered with Dovetail"
			continue
		}
		if worker.Draining {
			failed[name] = "draining"
			continue
		}
		if worker.Warming() {
			failed[name] = "registered but has not reported yet"
			continue
		}
		if !worker.Healthy(now, e.sched.Config().HeartbeatTimeout) {
			failed[name] = "heartbeat is stale"
			continue
		}
		available := e.sched.Available(name)
		if !request.Fits(available) {
			failed[name] = fmt.Sprintf("insufficient capacity: has %s, needs %s",
				available, request)
			continue
		}
		feasible = append(feasible, name)
	}

	sort.Strings(feasible)
	result := FilterResult{NodeNames: &feasible, FailedNodes: failed}
	e.respond(w, http.StatusOK, result)
}

func (e *Extender) handlePrioritize(w http.ResponseWriter, r *http.Request) {
	args, ok := e.decode(w, r)
	if !ok {
		return
	}
	request, err := PodResources(args.Pod)
	if err != nil {
		// Prioritize has no error field in the protocol; an empty list means
		// "no opinion", which lets kube-scheduler fall back to its own
		// scoring rather than failing the pod.
		e.respond(w, http.StatusOK, []HostPriority{})
		return
	}
	tenant := args.Pod.Metadata.Annotations[AnnotationTenant]

	names := nodeNames(args)
	out := make([]HostPriority, 0, len(names))
	for _, name := range names {
		score, known := e.sched.ScoreWorker(name, request, tenant, e.Clock())
		if !known {
			continue
		}
		// Kubernetes expects 0-10. Dovetail's scorer produces roughly 0-2
		// (packing + spread + a quarter of balance), so scale and clamp
		// rather than letting an out-of-range score be silently truncated by
		// kube-scheduler.
		scaled := int64(math.Round(score * 5))
		if scaled < 0 {
			scaled = 0
		}
		if scaled > 10 {
			scaled = 10
		}
		out = append(out, HostPriority{Host: name, Score: scaled})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Host < out[j].Host })
	e.respond(w, http.StatusOK, out)
}

// nodeNames extracts node names from either representation.
//
// The protocol allows both `nodes` (full objects) and `nodenames` (strings),
// selected by the extender's own configuration. Handling only one means
// working against exactly one kube-scheduler config, and the failure is a
// silent empty node list.
func nodeNames(args ExtenderArgs) []string {
	if args.NodeNames != nil && len(*args.NodeNames) > 0 {
		out := append([]string(nil), *args.NodeNames...)
		return out
	}
	if args.Nodes != nil {
		out := make([]string, 0, len(args.Nodes.Items))
		for _, n := range args.Nodes.Items {
			out = append(out, n.Metadata.Name)
		}
		return out
	}
	return nil
}

func (e *Extender) decode(w http.ResponseWriter, r *http.Request) (ExtenderArgs, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, 4<<20)
	var args ExtenderArgs
	// Unknown fields are NOT rejected here, unlike everywhere else in
	// Dovetail. kube-scheduler sends the full Pod and Node objects — hundreds
	// of fields — and this package deliberately models only the handful it
	// needs. Rejecting the rest would reject every real request.
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		e.respond(w, http.StatusBadRequest, FilterResult{
			Error: fmt.Sprintf("malformed extender args: %v", err),
		})
		return ExtenderArgs{}, false
	}
	return args, true
}

func (e *Extender) respond(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		e.log.Error("writing extender response", "error", err)
	}
}

// ---------------------------------------------------------------------------
// Pod ingestion
// ---------------------------------------------------------------------------

// SubmitPod converts and submits a pod to the scheduler.
//
// Idempotent on the pod key: resubmitting an already-known pod is reported as
// such rather than erroring, because a controller re-listing its pods is the
// normal case and not a fault.
func SubmitPod(sched *dovetail.Scheduler, pod Pod, now time.Time) (string, bool, error) {
	key := PodKey(pod)
	if pod.Spec.NodeName != "" {
		// Already bound. Scheduling it again would place a second copy.
		return key, false, fmt.Errorf("pod %s is already bound to node %s",
			key, pod.Spec.NodeName)
	}
	if _, exists := sched.Task(key); exists {
		return key, false, nil
	}
	job, err := JobFromPod(pod)
	if err != nil {
		return key, false, err
	}
	if err := sched.Submit(job, now); err != nil {
		return key, false, err
	}
	return key, true, nil
}

// PodStatusFor reports a submitted pod's current state.
func PodStatusFor(sched *dovetail.Scheduler, key string) (PodStatus, string, bool) {
	task, ok := sched.Task(key)
	if !ok {
		return PodStatus{}, "", false
	}
	return StatusForTask(task), task.WorkerID, true
}
