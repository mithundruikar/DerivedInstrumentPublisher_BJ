package com.bj.publisher.consumer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

final class ConsumerWaitSupport {
    private static final long IDLE_PARK_NANOS = TimeUnit.MILLISECONDS.toNanos(1L);

    private ConsumerWaitSupport() {
    }

    static void idle() {
        Thread.onSpinWait();
        LockSupport.parkNanos(IDLE_PARK_NANOS);
    }

    static void delayMillis(long delayMillis) {
        if (delayMillis <= 0L) {
            Thread.yield();
            return;
        }
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(delayMillis));
    }
}
