package com.bj.publisher.consumer;

import com.bj.publisher.entity.DerivedInstrumentUpdate;
import com.bj.publisher.metrics.PeriodicStatsLogger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Placeholder for durable journaling.
 * The journaling concept is intentionally kept, but replay implementation is deferred.
 */
public final class DurableJournalConsumer implements AutoCloseable, DerivedUpdateConsumer {
    private static final Logger LOGGER = LogManager.getLogger(DurableJournalConsumer.class);
    private final String name;
    private final Path journalPath;
    private final Path checkpointPath;
    private final long processingDelayMillis;
    private final java.util.function.Consumer<DerivedInstrumentUpdate> handler;
    private final PeriodicStatsLogger periodicStatsLogger;
    private final Thread worker;

    private volatile boolean running;
    private volatile long processedEvents;
    private volatile long missedUpdates;
    private volatile long lastProcessedSequence;
    private volatile long checkpointOffset;

    private DurableJournalConsumer(
            String name,
            Path journalPath,
            Path checkpointPath,
            long processingDelayMillis,
            long statsIntervalSeconds,
            java.util.function.Consumer<DerivedInstrumentUpdate> handler) {
        this.name = name;
        this.journalPath = journalPath;
        this.checkpointPath = checkpointPath;
        this.processingDelayMillis = processingDelayMillis;
        this.handler = handler;
        this.periodicStatsLogger = new PeriodicStatsLogger(
                "durable-consumer-" + name,
                statsIntervalSeconds,
                () -> LOGGER.info("name={} stats={}", name, stats()));
        this.worker = new Thread(this::runLoop, "durable-consumer-" + name);
        this.worker.setDaemon(true);
    }

    public static DurableJournalConsumer create(
            String name,
            Path journalPath,
            Path checkpointPath,
            long processingDelayMillis,
            long statsIntervalSeconds,
            java.util.function.Consumer<DerivedInstrumentUpdate> handler) {
        return new DurableJournalConsumer(
                name,
                journalPath,
                checkpointPath,
                processingDelayMillis,
                statsIntervalSeconds,
                handler);
    }

    public void start() {
        running = true;
        LOGGER.info(
                "Durable journaling is not implemented yet. Consumer '{}' will stay idle (journalPath={} checkpointPath={})",
                name,
                journalPath,
                checkpointPath);
        worker.start();
    }

    public DurableConsumerStats stats() {
        return new DurableConsumerStats(name, lastProcessedSequence, processedEvents, missedUpdates, checkpointOffset);
    }

    @Override
    public void onUpdate(DerivedInstrumentUpdate update) {
        handler.accept(update);
        lastProcessedSequence = update.derivedSequence();
        processedEvents++;
    }

    @Override
    public void close() {
        running = false;
        worker.interrupt();
        try {
            worker.join(TimeUnit.SECONDS.toMillis(1));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        periodicStatsLogger.close();
    }

    private void runLoop() {
        while (running) {
            ConsumerWaitSupport.delayMillis(Math.max(processingDelayMillis, 25L));
        }
    }
}
