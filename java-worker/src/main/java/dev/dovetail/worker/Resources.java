package dev.dovetail.worker;

/**
 * A resource vector: millicores and bytes.
 *
 * <p>A record, so it is immutable and has value equality. That matters more than it
 * looks: the worker subtracts and adds these on every task transition, and a mutable
 * vector shared between the executor and the heartbeat thread was the first bug I hit
 * here — the heartbeat reported a figure that was mid-update and the control plane
 * saw a machine with negative free capacity.
 *
 * <p>Integers throughout, matching the Go side. Floating point would drift across
 * thousands of add/subtract cycles and eventually report a worker as having
 * -0.0000001 cores free, which is a condition no comparison handles as expected.
 */
public record Resources(long milliCpu, long memBytes) {

    public static final Resources ZERO = new Resources(0, 0);

    public static Resources ofCores(double cores, double gib) {
        return new Resources((long) (cores * 1000), (long) (gib * 1024 * 1024 * 1024));
    }

    public Resources plus(Resources other) {
        return new Resources(milliCpu + other.milliCpu, memBytes + other.memBytes);
    }

    /**
     * Subtracts without clamping, so the result may be negative.
     *
     * <p>Not clamping is deliberate. A negative component means the worker's accounting
     * is wrong — it believes it is running more than it has — and clamping to zero
     * hides exactly that bug. {@link #hasNegative()} exists so callers can assert.
     */
    public Resources minus(Resources other) {
        return new Resources(milliCpu - other.milliCpu, memBytes - other.memBytes);
    }

    public Resources clamp() {
        return new Resources(Math.max(milliCpu, 0), Math.max(memBytes, 0));
    }

    /** Whether this vector fits inside {@code capacity} in every dimension. */
    public boolean fits(Resources capacity) {
        return milliCpu <= capacity.milliCpu && memBytes <= capacity.memBytes;
    }

    public boolean hasNegative() {
        return milliCpu < 0 || memBytes < 0;
    }

    public boolean isZero() {
        return milliCpu == 0 && memBytes == 0;
    }

    @Override
    public String toString() {
        return "{cpu=" + milliCpu + "m mem=" + (memBytes / (1024 * 1024)) + "MiB}";
    }
}
