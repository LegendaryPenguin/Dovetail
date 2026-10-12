package dovetail

import (
	"fmt"
	"math"
	"strings"
)

// Resources is a resource vector: what a job asks for, what a worker has, and
// what is currently committed.
//
// Two dimensions rather than an arbitrary map. A map would be more general and
// would make every arithmetic operation partial — "what is the sum of {cpu}
// and {cpu, mem}" has no good answer, and the code that answers it wrongly is
// the code that double-books a machine. Fixing the dimensions makes addition,
// subtraction and comparison total, and two is enough to make the multi-
// resource fairness problem real, which is the part worth building.
//
// Millicores and bytes, both integers. Floating point here would be a mistake:
// resource accounting sums and subtracts the same quantities thousands of
// times across scheduling cycles, and a vector that drifts by 1e-15 per cycle
// eventually reports a worker as having -0.0000001 CPU free, which is a
// condition no comparison handles the way you expect.
type Resources struct {
	// MilliCPU is CPU in thousandths of a core. 1000 is one core.
	MilliCPU int64
	// MemBytes is memory in bytes.
	MemBytes int64
}

// CPU is a convenience for whole cores.
func CPU(cores float64) int64 { return int64(cores * 1000) }

// GiB is a convenience for whole gibibytes.
func GiB(n float64) int64 { return int64(n * 1024 * 1024 * 1024) }

func (r Resources) Add(o Resources) Resources {
	return Resources{MilliCPU: r.MilliCPU + o.MilliCPU, MemBytes: r.MemBytes + o.MemBytes}
}

// Sub subtracts without clamping, so the result may be negative.
//
// Not clamping is deliberate. A negative component means the accounting is
// wrong — more has been committed than exists — and clamping to zero would
// hide exactly the bug that matters most here. Callers that need a floor say
// so with Clamp, and the scheduler's invariant checks look for negatives
// specifically.
func (r Resources) Sub(o Resources) Resources {
	return Resources{MilliCPU: r.MilliCPU - o.MilliCPU, MemBytes: r.MemBytes - o.MemBytes}
}

func (r Resources) Clamp() Resources {
	return Resources{MilliCPU: max64(r.MilliCPU, 0), MemBytes: max64(r.MemBytes, 0)}
}

func (r Resources) Scale(factor float64) Resources {
	return Resources{
		MilliCPU: int64(float64(r.MilliCPU) * factor),
		MemBytes: int64(float64(r.MemBytes) * factor),
	}
}

// Fits reports whether r can be satisfied out of capacity.
//
// Every dimension must fit. This is the whole of the feasibility question and
// it is deliberately not a score: a worker either has room or it does not, and
// mixing that with preference ordering is how a scheduler ends up placing work
// where it cannot run.
func (r Resources) Fits(capacity Resources) bool {
	return r.MilliCPU <= capacity.MilliCPU && r.MemBytes <= capacity.MemBytes
}

// IsZero reports whether the vector asks for nothing.
func (r Resources) IsZero() bool { return r.MilliCPU == 0 && r.MemBytes == 0 }

// HasNegative reports whether any dimension went below zero, which means the
// accounting is broken rather than merely tight.
func (r Resources) HasNegative() bool { return r.MilliCPU < 0 || r.MemBytes < 0 }

func (r Resources) String() string {
	return fmt.Sprintf("{cpu=%dm mem=%s}", r.MilliCPU, humanBytes(r.MemBytes))
}

// DominantShare returns the largest fraction of total that r occupies in any
// single dimension, and which dimension that was.
//
// This is the core of Dominant Resource Fairness: a tenant using 60% of the
// cluster's CPU and 10% of its memory has a dominant share of 60%, and
// fairness compares tenants on that number rather than on any single resource
// or on some weighted sum. The insight DRF encodes is that the resource you
// are most constrained by is the one that determines what you are really
// consuming — a memory-hungry tenant and a CPU-hungry tenant can both get
// "half the cluster" without either being starved, and neither a CPU-only nor
// a memory-only comparison can express that.
//
// A zero dimension in total contributes nothing rather than dividing by zero.
// If total is entirely zero the share is zero, because an empty cluster cannot
// be unfairly divided.
func (r Resources) DominantShare(total Resources) (share float64, dimension string) {
	share, dimension = 0, "none"
	if total.MilliCPU > 0 {
		if s := float64(r.MilliCPU) / float64(total.MilliCPU); s > share {
			share, dimension = s, "cpu"
		}
	}
	if total.MemBytes > 0 {
		if s := float64(r.MemBytes) / float64(total.MemBytes); s > share {
			share, dimension = s, "mem"
		}
	}
	return share, dimension
}

// Utilization returns the per-dimension fraction of total that r occupies.
//
// Separate from DominantShare because the two answer different questions.
// Fairness wants one comparable number per tenant; an operator looking at
// cluster health wants to know that CPU is at 95% while memory sits at 30%,
// and a dominant share of 0.95 hides the imbalance that explains why nothing
// is scheduling.
func (r Resources) Utilization(total Resources) (cpu, mem float64) {
	if total.MilliCPU > 0 {
		cpu = float64(r.MilliCPU) / float64(total.MilliCPU)
	}
	if total.MemBytes > 0 {
		mem = float64(r.MemBytes) / float64(total.MemBytes)
	}
	return cpu, mem
}

// Validate rejects a request that cannot be scheduled on principle.
//
// Checked at submission rather than at placement. A job asking for negative
// memory would otherwise sit in the queue forever being filtered out by every
// worker, and the operator would see "pending, no suitable node" rather than
// "this request is nonsense".
func (r Resources) Validate() error {
	if r.MilliCPU < 0 || r.MemBytes < 0 {
		return fmt.Errorf("resource request has a negative dimension: %s", r)
	}
	if r.IsZero() {
		// A zero request would be placeable on a full worker, unbounded times.
		// Scheduling is about capacity, so asking for none is a category error.
		return fmt.Errorf("resource request is empty: a task must ask for something")
	}
	return nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// Min returns the per-dimension minimum, which is the largest vector that
// fits in both inputs.
func (r Resources) Min(o Resources) Resources {
	return Resources{MilliCPU: min64(r.MilliCPU, o.MilliCPU), MemBytes: min64(r.MemBytes, o.MemBytes)}
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < 0 {
		return "-" + humanBytes(-b)
	}
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	exp, div := 0, int64(unit)
	for n := b / unit; n >= unit && exp < 4; n /= unit {
		div *= unit
		exp++
	}
	val := float64(b) / float64(div)
	suffix := [...]string{"KiB", "MiB", "GiB", "TiB", "PiB"}[exp]
	if val == math.Trunc(val) {
		return fmt.Sprintf("%.0f%s", val, suffix)
	}
	return fmt.Sprintf("%.1f%s", val, suffix)
}

// FormatShares renders a share map for logs and the simulator's reports,
// sorted so successive lines are comparable by eye.
func FormatShares(shares map[string]float64) string {
	keys := sortedKeys(shares)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "%s=%.3f", k, shares[k])
	}
	return b.String()
}
