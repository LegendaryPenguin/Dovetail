package dev.dovetail.worker;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The worker: registers, heartbeats, runs tasks, reports outcomes.
 *
 * <p>The heartbeat loop is the only thread that talks to the control plane, and that
 * is a deliberate constraint rather than a simplification. Letting task completion
 * report inline from an executor thread was my first design, and under load it
 * produced interleaved reports that arrived out of order with the heartbeat's running
 * list — so the control plane would see a task reported running in one request and
 * absent from a heartbeat sent microseconds earlier, and reconcile it away. Funnelling
 * every outbound call through one thread makes the worker's story about itself
 * monotonic.
 *
 * <p>Completions are therefore queued and drained by the heartbeat loop. The cost is
 * up to one heartbeat interval of reporting latency on a finished task, which is
 * cheaper than the alternative by a wide margin.
 */
public final class Worker implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(Worker.class.getName());

    private final String workerId;
    private final ControlPlaneClient client;
    private final TaskRegistry registry;
    private final TaskRunner runner;
    private final String zone;
    private final String rack;

    private final ExecutorService taskPool;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Completions awaiting report, drained by the heartbeat loop.
     *
     * <p>Unbounded is wrong in general but right here: the queue is bounded in practice
     * by the number of tasks the worker can hold, which is bounded by its capacity.
     * A bounded queue would mean dropping a completion report, and a dropped
     * completion becomes a ghost the control plane has to reconcile away — strictly
     * worse than a brief memory cost.
     */
    private final java.util.concurrent.ConcurrentLinkedQueue<Completion> completions =
            new java.util.concurrent.ConcurrentLinkedQueue<>();

    private record Completion(String taskId, boolean success, String error) {}

    /** What actually executes a task. Injected so tests do not need real work. */
    public interface TaskRunner {
        /** Runs the task, returning normally on success and throwing on failure. */
        void run(String taskId, String tenant, Resources request) throws Exception;
    }

    public Worker(String workerId, ControlPlaneClient client, Resources capacity,
                  String zone, String rack, TaskRunner runner, int maxConcurrent) {
        this.workerId = workerId;
        this.client = client;
        this.registry = new TaskRegistry(capacity);
        this.runner = runner;
        this.zone = zone;
        this.rack = rack;
        this.taskPool = Executors.newFixedThreadPool(Math.max(1, maxConcurrent), r -> {
            Thread t = new Thread(r, "dovetail-task");
            // Daemon threads so a wedged task cannot keep the JVM alive past
            // shutdown. The control plane will reconcile anything left behind.
            t.setDaemon(true);
            return t;
        });
    }

    public TaskRegistry registry() {
        return registry;
    }

    /**
     * Runs the worker until {@link #close()}.
     *
     * <p>Re-registers automatically on a 409. A control plane that restarted and lost
     * its registry is a normal event, and a worker that gave up on it would have to be
     * restarted by hand across a whole fleet.
     */
    public void run() throws InterruptedException {
        running.set(true);
        Duration interval = Duration.ofSeconds(5);

        while (running.get()) {
            try {
                interval = client.register(registry.capacity(), zone, rack).heartbeatInterval();
                LOG.log(Level.INFO, "registered {0}: capacity {1}, heartbeat every {2}",
                        new Object[]{workerId, registry.capacity(), interval});
                heartbeatLoop(interval);
            } catch (ControlPlaneClient.NotRegisteredException e) {
                LOG.log(Level.WARNING, "control plane lost our registration; re-registering");
            } catch (IOException e) {
                LOG.log(Level.WARNING, "control plane unreachable: {0}", e.getMessage());
            }
            if (running.get()) {
                // Jittered backoff. Without jitter, a fleet that loses the control
                // plane together reconnects together and knocks it over again — the
                // thundering herd that turns a brief outage into a sustained one.
                long base = Math.max(interval.toMillis(), 1000);
                long jittered = base / 2 + ThreadLocalRandom.current().nextLong(base);
                Thread.sleep(jittered);
            }
        }
    }

    private void heartbeatLoop(Duration interval) throws IOException, InterruptedException {
        while (running.get()) {
            // Report completions before the heartbeat, so the running list we send
            // cannot still contain a task we have already finished.
            drainCompletions();

            Resources free = registry.free();
            List<String> reportable = registry.reportable();

            ControlPlaneClient.HeartbeatResponse resp = client.heartbeat(free, reportable);

            for (String taskId : resp.cancel()) {
                registry.cancel(taskId).ifPresent(e ->
                        LOG.log(Level.INFO, "control plane cancelled {0}", e.taskId()));
            }

            for (ControlPlaneClient.Assignment a : resp.assign()) {
                if (!registry.accept(a.taskId(), a.tenant(), a.request(), a.epoch())) {
                    // Refusals are expected and uninteresting individually: a
                    // duplicate, a stale epoch, or genuinely no room. Logged at fine
                    // so a debugging session can see them without a steady-state
                    // worker being noisy.
                    LOG.log(Level.FINE, "declined assignment {0}", a.taskId());
                    continue;
                }
                submit(a);
            }

            Thread.sleep(interval.toMillis());
        }
    }

    private void submit(ControlPlaneClient.Assignment a) {
        taskPool.submit(() -> {
            // markRunning before reporting started, so a heartbeat racing this
            // cannot see the task as accepted-but-not-running after the control
            // plane has been told it started.
            registry.markRunning(a.taskId());
            try {
                client.taskStarted(a.taskId());
            } catch (Exception e) {
                // The control plane will learn from the heartbeat's running list
                // instead. Not fatal, and not worth failing the task over.
                LOG.log(Level.FINE, "could not report start of {0}: {1}",
                        new Object[]{a.taskId(), e.getMessage()});
            }

            boolean success = true;
            String error = null;
            try {
                runner.run(a.taskId(), a.tenant(), a.request());
            } catch (Exception e) {
                success = false;
                error = e.getClass().getSimpleName() + ": " + e.getMessage();
            } catch (Throwable t) {
                // An Error — OOM, StackOverflow — is still a task outcome the
                // control plane needs. Caught so the capacity is released rather
                // than the task becoming a ghost, then rethrown is *not* done
                // deliberately: killing the pool thread would leak capacity for
                // every subsequent task.
                success = false;
                error = "fatal: " + t;
            } finally {
                registry.complete(a.taskId());
                completions.add(new Completion(a.taskId(), success, error));
            }
        });
    }

    private void drainCompletions() {
        Completion c;
        while ((c = completions.poll()) != null) {
            try {
                client.taskFinished(c.taskId(), c.success(), c.error());
            } catch (Exception e) {
                // Put it back and try on the next heartbeat. Dropping it would
                // leave the control plane holding a reservation for finished work,
                // which it only reclaims via reconciliation.
                completions.add(c);
                LOG.log(Level.FINE, "could not report completion of {0}", c.taskId());
                return;
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        taskPool.shutdown();
        try {
            if (!taskPool.awaitTermination(5, TimeUnit.SECONDS)) {
                taskPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            taskPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** A runner that sleeps, for demos and load generation. */
    public static TaskRunner sleepRunner(Duration mean, double failureRate) {
        return (taskId, tenant, request) -> {
            long millis = (long) (ThreadLocalRandom.current().nextExponential() * mean.toMillis());
            Thread.sleep(Math.max(1, millis));
            if (ThreadLocalRandom.current().nextDouble() < failureRate) {
                throw new IllegalStateException("synthetic failure");
            }
        };
    }

    public static void main(String[] args) throws Exception {
        String server = System.getenv().getOrDefault("DOVETAIL_SERVER", "http://127.0.0.1:8080");
        String id = System.getenv().getOrDefault("DOVETAIL_WORKER_ID", "java-worker-1");
        String zone = System.getenv().getOrDefault("DOVETAIL_ZONE", "a");
        double cores = Double.parseDouble(System.getenv().getOrDefault("DOVETAIL_CORES", "8"));
        double gib = Double.parseDouble(System.getenv().getOrDefault("DOVETAIL_MEM_GIB", "32"));

        Resources capacity = Resources.ofCores(cores, gib);
        ControlPlaneClient client = new ControlPlaneClient(server, id);
        try (Worker w = new Worker(id, client, capacity, zone, "r1",
                sleepRunner(Duration.ofSeconds(3), 0.02), 32)) {
            Runtime.getRuntime().addShutdownHook(new Thread(w::close));
            w.run();
        }
    }
}
