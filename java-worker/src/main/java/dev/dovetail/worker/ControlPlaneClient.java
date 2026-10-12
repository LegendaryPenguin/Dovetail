package dev.dovetail.worker;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * HTTP client for the Go control plane.
 *
 * <p>Hand-rolled JSON, no Jackson. The protocol is six endpoints with flat objects,
 * and the dependency would be larger than the code it replaces. More importantly it
 * keeps the worker a single jar with no shaded dependencies, which is what makes it
 * deployable as a sidecar without a build system argument.
 *
 * <p>The design decision that matters here: <strong>the worker pulls assignments on its
 * heartbeat response</strong> rather than listening for pushes. The control plane
 * therefore needs no inbound route to any worker, which is what lets workers sit
 * behind NAT. The cost is that assignment latency is bounded below by the heartbeat
 * interval, which the simulator measures.
 */
public final class ControlPlaneClient {

    private final HttpClient http;
    private final String baseUrl;
    private final String workerId;

    public ControlPlaneClient(String baseUrl, String workerId) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.workerId = workerId;
        this.http = HttpClient.newBuilder()
                // A connect timeout, because the default is none and a worker that
                // hangs on connect stops heartbeating without ever reporting an
                // error — which looks identical to a crashed worker.
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    /** Thrown when the control plane does not recognise this worker. */
    public static final class NotRegisteredException extends IOException {
        public NotRegisteredException(String message) {
            super(message);
        }
    }

    /** Registration response: the control plane dictates the heartbeat interval. */
    public record Registration(Duration heartbeatInterval) {}

    public Registration register(Resources capacity, String zone, String rack) throws IOException, InterruptedException {
        String body = "{"
                + "\"worker_id\":" + quote(workerId) + ","
                + "\"capacity\":{\"milli_cpu\":" + capacity.milliCpu() + ",\"mem_bytes\":" + capacity.memBytes() + "},"
                + "\"zone\":" + quote(zone) + ","
                + "\"rack\":" + quote(rack)
                + "}";
        String response = post("/register", body);
        long millis = (long) readNumber(response, "heartbeat_interval_millis");
        if (millis <= 0) {
            // Defend against a control plane that reports nonsense rather than
            // spinning at zero delay, which would turn one bad response into a
            // denial of service against the control plane.
            millis = 5000;
        }
        return new Registration(Duration.ofMillis(millis));
    }

    /** What the control plane wants the worker to do, returned on each heartbeat. */
    public record HeartbeatResponse(List<Assignment> assign, List<String> cancel, long epoch) {}

    public record Assignment(String taskId, String tenant, Resources request, long epoch) {}

    public HeartbeatResponse heartbeat(Resources free, List<String> running)
            throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"worker_id\":").append(quote(workerId))
                .append(",\"free\":{\"milli_cpu\":").append(free.milliCpu())
                .append(",\"mem_bytes\":").append(free.memBytes()).append("}")
                .append(",\"running\":[");
        for (int i = 0; i < running.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(quote(running.get(i)));
        }
        sb.append("]}");

        String response = post("/heartbeat", sb.toString());
        return new HeartbeatResponse(
                parseAssignments(response),
                parseStringArray(response, "cancel"),
                (long) readNumber(response, "epoch"));
    }

    public void taskStarted(String taskId) throws IOException, InterruptedException {
        post("/task/started",
                "{\"task_id\":" + quote(taskId) + ",\"worker_id\":" + quote(workerId) + "}");
    }

    public void taskFinished(String taskId, boolean success, String error)
            throws IOException, InterruptedException {
        post("/task/finished",
                "{\"task_id\":" + quote(taskId)
                        + ",\"success\":" + success
                        + ",\"error\":" + quote(error == null ? "" : error) + "}");
    }

    private String post(String path, String body) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() == 409) {
            // The control plane does not know this worker — it restarted and lost
            // its registry while we kept running. Distinguished from other errors
            // because the correct response is to re-register, not to retry.
            throw new NotRegisteredException("control plane does not recognise worker " + workerId);
        }
        if (resp.statusCode() / 100 != 2) {
            throw new IOException("POST " + path + " returned " + resp.statusCode() + ": " + resp.body());
        }
        return resp.body();
    }

    // -----------------------------------------------------------------------
    // Minimal JSON reading
    //
    // Deliberately narrow: it reads the specific shapes this protocol uses and
    // nothing else. A general parser would be more code than the client. It is
    // position-based rather than structural, which is why the Go side rejects
    // unknown fields — a protocol change breaks loudly at the server instead of
    // being silently misread here.
    // -----------------------------------------------------------------------

    private static String quote(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    static double readNumber(String json, String key) {
        int i = json.indexOf("\"" + key + "\"");
        if (i < 0) {
            return 0;
        }
        i = json.indexOf(':', i);
        if (i < 0) {
            return 0;
        }
        int start = i + 1;
        while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
            start++;
        }
        int end = start;
        while (end < json.length() && "+-0123456789.eE".indexOf(json.charAt(end)) >= 0) {
            end++;
        }
        if (end == start) {
            return 0;
        }
        try {
            return Double.parseDouble(json.substring(start, end));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    static List<String> parseStringArray(String json, String key) {
        List<String> out = new ArrayList<>();
        int i = json.indexOf("\"" + key + "\"");
        if (i < 0) {
            return out;
        }
        int open = json.indexOf('[', i);
        int close = json.indexOf(']', open);
        if (open < 0 || close < 0) {
            return out;
        }
        String inner = json.substring(open + 1, close);
        int pos = 0;
        while (true) {
            int q1 = inner.indexOf('"', pos);
            if (q1 < 0) {
                break;
            }
            int q2 = inner.indexOf('"', q1 + 1);
            if (q2 < 0) {
                break;
            }
            out.add(inner.substring(q1 + 1, q2));
            pos = q2 + 1;
        }
        return out;
    }

    static List<Assignment> parseAssignments(String json) {
        List<Assignment> out = new ArrayList<>();
        int i = json.indexOf("\"assign\"");
        if (i < 0) {
            return out;
        }
        int open = json.indexOf('[', i);
        if (open < 0) {
            return out;
        }
        // Walk objects within the array, tracking brace depth so a nested
        // "request" object does not terminate the element early.
        int depth = 0;
        int objStart = -1;
        for (int p = open; p < json.length(); p++) {
            char c = json.charAt(p);
            if (c == '{') {
                if (depth == 0) {
                    objStart = p;
                }
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0 && objStart >= 0) {
                    String obj = json.substring(objStart, p + 1);
                    out.add(new Assignment(
                            readString(obj, "task_id"),
                            readString(obj, "tenant"),
                            new Resources((long) readNumber(obj, "milli_cpu"), (long) readNumber(obj, "mem_bytes")),
                            (long) readNumber(obj, "epoch")));
                    objStart = -1;
                }
            } else if (c == ']' && depth == 0) {
                break;
            }
        }
        return out;
    }

    static String readString(String json, String key) {
        int i = json.indexOf("\"" + key + "\"");
        if (i < 0) {
            return "";
        }
        int colon = json.indexOf(':', i);
        int q1 = json.indexOf('"', colon);
        if (q1 < 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int p = q1 + 1; p < json.length(); p++) {
            char c = json.charAt(p);
            if (c == '\\' && p + 1 < json.length()) {
                char n = json.charAt(++p);
                switch (n) {
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    default -> sb.append(n);
                }
                continue;
            }
            if (c == '"') {
                break;
            }
            sb.append(c);
        }
        return sb.toString();
    }
}
