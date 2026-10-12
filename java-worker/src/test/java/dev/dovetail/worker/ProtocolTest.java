package dev.dovetail.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for the hand-rolled JSON reading.
 *
 * <p>These exist because the parser is narrow by design and therefore brittle by
 * design. The tradeoff is only defensible if the shapes it must handle are pinned,
 * and in particular the nested-object case — a "request" object inside an assignment
 * — is where a naive brace scan gets it wrong.
 */
class ProtocolTest {

    @Test
    void readsNumbersIncludingNested() {
        String json = "{\"epoch\":42,\"heartbeat_interval_millis\":5000}";
        assertEquals(42, (long) ControlPlaneClient.readNumber(json, "epoch"));
        assertEquals(5000, (long) ControlPlaneClient.readNumber(json, "heartbeat_interval_millis"));
    }

    @Test
    void missingNumberIsZeroNotAnException() {
        // A control plane that omits a field should not crash the worker; the
        // caller substitutes a safe default.
        assertEquals(0, (long) ControlPlaneClient.readNumber("{}", "epoch"));
    }

    @Test
    void readsStringArrays() {
        String json = "{\"cancel\":[\"t1\",\"t2\",\"t3\"],\"epoch\":1}";
        assertEquals(List.of("t1", "t2", "t3"), ControlPlaneClient.parseStringArray(json, "cancel"));
    }

    @Test
    void emptyAndNullArraysBothReadAsEmpty() {
        assertTrue(ControlPlaneClient.parseStringArray("{\"cancel\":[]}", "cancel").isEmpty());
        assertTrue(ControlPlaneClient.parseStringArray("{\"epoch\":1}", "cancel").isEmpty());
    }

    @Test
    void parsesAssignmentsWithNestedResourceObjects() {
        // The case a naive brace scan breaks on: the inner "request" object's
        // closing brace must not terminate the assignment element.
        String json = "{\"assign\":["
                + "{\"task_id\":\"a-t0\",\"tenant\":\"acme\",\"request\":{\"milli_cpu\":2000,\"mem_bytes\":4096},\"epoch\":7},"
                + "{\"task_id\":\"a-t1\",\"tenant\":\"acme\",\"request\":{\"milli_cpu\":1000,\"mem_bytes\":2048},\"epoch\":7}"
                + "],\"cancel\":[],\"epoch\":7}";

        List<ControlPlaneClient.Assignment> got = ControlPlaneClient.parseAssignments(json);
        assertEquals(2, got.size(), "both assignments must be parsed");
        assertEquals("a-t0", got.get(0).taskId());
        assertEquals("acme", got.get(0).tenant());
        assertEquals(2000, got.get(0).request().milliCpu());
        assertEquals(4096, got.get(0).request().memBytes());
        assertEquals(7, got.get(0).epoch());
        assertEquals("a-t1", got.get(1).taskId());
        assertEquals(1000, got.get(1).request().milliCpu());
    }

    @Test
    void absentAssignListParsesAsEmpty() {
        assertTrue(ControlPlaneClient.parseAssignments("{\"cancel\":[],\"epoch\":3}").isEmpty());
    }

    @Test
    void readsStringsWithEscapes() {
        String json = "{\"error\":\"line one\\nline \\\"two\\\"\"}";
        assertEquals("line one\nline \"two\"", ControlPlaneClient.readString(json, "error"));
    }

    @Test
    void resourceArithmeticDoesNotClampSilently() {
        // A negative dimension means the accounting is wrong, and hiding it would
        // conceal the bug that matters most.
        Resources r = Resources.ofCores(1, 1).minus(Resources.ofCores(2, 2));
        assertTrue(r.hasNegative());
        assertFalse(r.clamp().hasNegative());
    }

    @Test
    void fitsRequiresEveryDimension() {
        Resources capacity = Resources.ofCores(4, 8);
        assertTrue(Resources.ofCores(4, 8).fits(capacity));
        assertFalse(Resources.ofCores(5, 1).fits(capacity), "CPU alone exceeding must fail");
        assertFalse(Resources.ofCores(1, 9).fits(capacity), "memory alone exceeding must fail");
    }
}
