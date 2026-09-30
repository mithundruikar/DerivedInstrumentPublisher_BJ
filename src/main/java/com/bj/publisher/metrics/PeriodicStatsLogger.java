package com.bj.publisher.metrics;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class PeriodicStatsLogger implements AutoCloseable {
    private final ScheduledExecutorService executor;

    public PeriodicStatsLogger(String name, long intervalSeconds, Runnable action) {
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, name + "-stats");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleAtFixedRate(action, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
