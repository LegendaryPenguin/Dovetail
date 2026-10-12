package dev.dovetail.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TaskRegistryTest {

    private static Resources req(double cores, double gib) {
        return Resources.ofCores(cores, gib);
    }

    private static TaskRegistry registry() {
        return new TaskRegistry(Resources.ofCores(8, 32));
    }

    @Test
    void acceptsUpToCapacityAndNoFurther() {
        TaskRegistry r = registry();
        // Four tasks of 2 CPU fit in 8 CPU; a fifth must not.
        for (int i = 0; i < 4; i++) {
            assertTrue(r.accept("t" + i, "acme", req(2, 8), 1), "task " + i + " should fit");
        }
        assertFalse(r.accept("t4", "acme", req(2, 8), 1),
                "a fifth task must be refused: accepting would oversubscribe the machine");
        assertTrue(r.free().isZero(), "free should be zero, was " + r.free());
    }

    @Test
    void acceptedTasksCountAgainstFreeCapacityBeforeTheyStart() {
        // The worker-side mirror of the control plane's double-booking bug. A
        // worker reporting capacity as free while holding an accepted-but-unstarted
        // task invites the control plane to place something else into the same gap.
        TaskRegistry r = registry();
        assertTrue(r.accept("t1", "acme", req(4, 16), 1));
        assertEquals(Resources.ofCores(4, 16), r.free(),
                "an accepted task must reduce reported free capacity immediately");
    }

    @Test
    void duplicateAssignmentIsRefusedIdempotently() {
        // The control plane re-sends assignments it has not seen confirmed. That is
        // normal, not an error, and must not start a second copy.
        TaskRegistry r = registry();
        assertTrue(r.accept("t1", "acme", req(1, 1), 1));
        assertFalse(r.accept("t1", "acme", req(1, 1), 1));
        assertEquals(1, r.size());
    }

    @Test
    void staleEpochIsRefused() {
        // A replayed command from before the newest epoch may already have been
        // reassigned elsewhere; running it risks a second live copy.
        TaskRegistry r = registry();
        assertTrue(r.accept("t1", "acme", req(1, 1), 10));
        assertFalse(r.accept("t2", "acme", req(1, 1), 5), "epoch 5 is older than the seen epoch 10");
        assertTrue(r.accept("t3", "acme", req(1, 1), 10), "the same epoch is still acceptable");
        assertTrue(r.accept("t4", "acme", req(1, 1), 11), "a newer epoch is acceptable");
    }

    @Test
    void completedTaskCannotBeRestarted() {
        // The worst failure this system can produce is running a non-idempotent
        // task twice. A completed task is remembered specifically to prevent it.
        TaskRegistry r = registry();
        assertTrue(r.accept("t1", "acme", req(1, 1), 1));
        assertTrue(r.complete("t1").isPresent());
        assertTrue(r.hasCompleted("t1"));
        assertFalse(r.accept("t1", "acme", req(1, 1), 1),
                "a replayed assignment for a finished task must not re-run it");
    }

    @Test
    void completionReleasesCapacity() {
        TaskRegistry r = registry();
        Resources capacity = r.capacity();
        assertTrue(r.accept("t1", "acme", req(4, 16), 1));
        assertFalse(r.free().equals(capacity));
        r.complete("t1");
        assertEquals(capacity, r.free(), "completing a task must return its capacity");
    }

    @Test
    void cancellationReleasesCapacityWithoutRecordingCompletion() {
        // A cancelled task never ran, so it must remain assignable later.
        TaskRegistry r = registry();
        Resources capacity = r.capacity();
        assertTrue(r.accept("t1", "acme", req(4, 16), 1));
        assertTrue(r.cancel("t1").isPresent());
        assertEquals(capacity, r.free());
        assertFalse(r.hasCompleted("t1"));
        assertTrue(r.accept("t1", "acme", req(4, 16), 1), "a cancelled task should be re-assignable");
    }

    @Test
    void reportableIncludesAcceptedNotOnlyRunning() {
        // Omitting accepted tasks would make the control plane believe its
        // assignment was lost and expire a reservation for work about to start.
        TaskRegistry r = registry();
        assertTrue(r.accept("t1", "acme", req(1, 1), 1));
        assertTrue(r.accept("t2", "acme", req(1, 1), 1));
        r.markRunning("t2");
        assertEquals(List.of("t1", "t2"), r.reportable());
    }

    @Test
    void reportableIsSortedForStableHeartbeats() {
        TaskRegistry r = registry();
        for (String id : List.of("t9", "t1", "t5")) {
            assertTrue(r.accept(id, "acme", req(1, 1), 1));
        }
        assertEquals(List.of("t1", "t5", "t9"), r.reportable());
    }

    @Test
    void freeNeverGoesNegative() {
        TaskRegistry r = registry();
        for (int i = 0; i < 20; i++) {
            r.accept("t" + i, "acme", req(1, 4), 1);
        }
        assertFalse(r.free().hasNegative(), "free was " + r.free());
    }

    @Test
    void concurrentAssignmentsDoNotOversubscribe() throws Exception {
        // The compound operation "is this known, and if not reserve capacity and
        // record it" has to be atomic. A ConcurrentHashMap would make each map
        // operation safe and leave this wrong, which is why the class is
        // synchronized instead.
        TaskRegistry r = new TaskRegistry(Resources.ofCores(10, 40));
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            final int n = i;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (r.accept("t" + n, "acme", req(1, 4), 1)) {
                    accepted.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(10, accepted.get(),
                "exactly 10 tasks of 1 CPU fit in 10 CPU, regardless of thread interleaving");
        assertFalse(r.free().hasNegative());
    }

    @Test
    void sameTaskFromTwoThreadsIsAcceptedOnce() {
        TaskRegistry r = registry();
        int threads = 8;
        AtomicInteger accepted = new AtomicInteger();
        List<Thread> ts = new java.util.ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(() -> {
                if (r.accept("same", "acme", req(1, 1), 1)) {
                    accepted.incrementAndGet();
                }
            });
            ts.add(t);
            t.start();
        }
        ts.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertEquals(1, accepted.get(), "the same task id must be accepted exactly once");
    }
}
