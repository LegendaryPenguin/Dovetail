package dev.dovetail.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The worker's own record of what it is running.
 *
 * <p>This is the authoritative answer to "what is executing on this machine", and the
 * control plane defers to it during reconciliation. That asymmetry is the whole
 * contract: the control plane owns <em>intent</em>, the worker owns <em>execution</em>,
 * and reconciliation only works if each is believed about the thing it actually knows.
 *
 * <p>Synchronized rather than using concurrent collections. Three threads touch this —
 * the heartbeat loop, the executor completion callback, and the assignment handler —
 * and the operations that matter are multi-step: "is this task known, and if not,
 * reserve capacity and record it" has to be atomic or two assignments for the same
 * task both succeed. A ConcurrentHashMap would make each individual map operation safe
 * and the compound operation still wrong.
 */
public final class TaskRegistry {

    /** What the worker believes about one task. */
    public record Entry(String taskId, String tenant, Resources request, long epoch, State state) {
        public enum State {
            /** Accepted, not yet started executing. */
            ACCEPTED,
            /** Executing now. Counted against free capacity. */
            RUNNING,
            /** Finished; awaiting acknowledgement from the control plane. */
            REPORTED
        }
    }

    private final Resources capacity;
    private final Map<String, Entry> entries = new HashMap<>();

    /**
     * Epochs the worker has seen, newest first.
     *
     * <p>Used to reject a replayed stale assignment. Without it, a control plane that
     * retries an assignment command after the task has already finished causes the
     * worker to run it a second time — which for a non-idempotent task is the worst
     * failure this system can produce.
     */
    private long highestEpoch = -1;

    private final Set<String> completed = new LinkedHashSet<>();

    /** Bounds the completed-task memory so a long-lived worker does not grow forever. */
    private static final int COMPLETED_HISTORY = 4096;

    public TaskRegistry(Resources capacity) {
        this.capacity = capacity;
    }

    public Resources capacity() {
        return capacity;
    }

    /**
     * Records an assignment, or explains why it was refused.
     *
     * <p>Returns false rather than throwing for the expected refusals — a duplicate
     * assignment and a stale epoch are both normal consequences of a retrying control
     * plane, not errors.
     */
    public synchronized boolean accept(String taskId, String tenant, Resources request, long epoch) {
        if (completed.contains(taskId)) {
            // Already run and reported. Accepting again would execute it twice.
            return false;
        }
        if (entries.containsKey(taskId)) {
            // Already have it; the control plane is re-sending because it has not
            // seen our confirmation yet. Idempotent, not an error.
            return false;
        }
        if (epoch < highestEpoch) {
            // A command from before the newest one we have seen. The control plane
            // has moved on, so this assignment may already have been reassigned
            // elsewhere, and running it risks a second live copy.
            return false;
        }
        if (!request.fits(free())) {
            // The control plane thought there was room and the worker disagrees.
            // Refusing is correct: the control plane's reservation will lapse and it
            // will place the task somewhere that fits, whereas accepting would
            // oversubscribe the machine.
            return false;
        }
        highestEpoch = Math.max(highestEpoch, epoch);
        entries.put(taskId, new Entry(taskId, tenant, request, epoch, Entry.State.ACCEPTED));
        return true;
    }

    public synchronized boolean markRunning(String taskId) {
        Entry e = entries.get(taskId);
        if (e == null || e.state() == Entry.State.REPORTED) {
            return false;
        }
        entries.put(taskId, new Entry(e.taskId(), e.tenant(), e.request(), e.epoch(), Entry.State.RUNNING));
        return true;
    }

    /** Removes a task and remembers that it ran, so a replay cannot restart it. */
    public synchronized Optional<Entry> complete(String taskId) {
        Entry e = entries.remove(taskId);
        if (e == null) {
            return Optional.empty();
        }
        completed.add(taskId);
        if (completed.size() > COMPLETED_HISTORY) {
            // LinkedHashSet preserves insertion order, so this drops the oldest.
            var it = completed.iterator();
            it.next();
            it.remove();
        }
        return Optional.of(e);
    }

    /** Drops a task without recording it as completed, for a cancellation. */
    public synchronized Optional<Entry> cancel(String taskId) {
        return Optional.ofNullable(entries.remove(taskId));
    }

    /**
     * Free capacity: total minus everything accepted or running.
     *
     * <p>Accepted tasks count even though they have not started. A worker that reported
     * capacity as free while holding an accepted-but-unstarted task would invite the
     * control plane to place something else into the same gap, which is the
     * worker-side mirror of the double-booking bug the Go ledger exists to prevent.
     */
    public synchronized Resources free() {
        Resources used = Resources.ZERO;
        for (Entry e : entries.values()) {
            if (e.state() != Entry.State.REPORTED) {
                used = used.plus(e.request());
            }
        }
        return capacity.minus(used).clamp();
    }

    /**
     * Task IDs to report in a heartbeat.
     *
     * <p>Includes ACCEPTED as well as RUNNING. The control plane uses this list to
     * confirm reservations, and omitting accepted tasks would make it believe its
     * assignment was lost and expire a reservation for work the worker is about to
     * start.
     */
    public synchronized List<String> reportable() {
        List<String> out = new ArrayList<>();
        for (Entry e : entries.values()) {
            if (e.state() != Entry.State.REPORTED) {
                out.add(e.taskId());
            }
        }
        out.sort(String::compareTo);
        return out;
    }

    public synchronized Optional<Entry> get(String taskId) {
        return Optional.ofNullable(entries.get(taskId));
    }

    public synchronized int size() {
        return entries.size();
    }

    public synchronized boolean hasCompleted(String taskId) {
        return completed.contains(taskId);
    }
}
