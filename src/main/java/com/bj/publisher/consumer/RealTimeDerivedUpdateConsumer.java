package com.bj.publisher.consumer;

import com.bj.publisher.entity.DerivedInstrumentUpdate;
import com.bj.publisher.metrics.PeriodicStatsLogger;
import com.bj.publisher.ringbuffer.DerivedUpdateRingBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public final class RealTimeDerivedUpdateConsumer implements AutoCloseable, DerivedUpdateConsumer {
    private static final Logger LOGGER = LogManager.getLogger(RealTimeDerivedUpdateConsumer.class);
    private final String name;
    private final RingBufferReader ringBufferReader;
    private final long processingDelayMillis;
    private final java.util.function.Consumer<DerivedInstrumentUpdate> handler;
    private final PeriodicStatsLogger periodicStatsLogger;
    private final Thread worker;

    private volatile boolean running;
    private volatile long processedEvents;

    private RealTimeDerivedUpdateConsumer(
            String name,
            DerivedUpdateRingBuffer ringBuffer,
            long processingDelayMillis,
            long statsIntervalSeconds,
            java.util.function.Consumer<DerivedInstrumentUpdate> handler) {
        this.name = name;
        this.ringBufferReader = new RingBufferReader(name, ringBuffer, this);
        this.processingDelayMillis = processingDelayMillis;
        this.handler = handler;
        this.periodicStatsLogger = new PeriodicStatsLogger(
                "realtime-consumer-" + name,
                statsIntervalSeconds,
                () -> LOGGER.info("name={} stats={}", name, stats()));
        this.worker = new Thread(this::runLoop, "realtime-consumer-" + name);
        this.worker.setDaemon(true);
    }

    public static RealTimeDerivedUpdateConsumer create(
            String name,
            DerivedUpdateRingBuffer ringBuffer,
            long processingDelayMillis,
            long statsIntervalSeconds,
            java.util.function.Consumer<DerivedInstrumentUpdate> handler) {
        return new RealTimeDerivedUpdateConsumer(name, ringBuffer, processingDelayMillis, statsIntervalSeconds, handler);
    }

    public void start() {
        running = true;
        worker.start();
    }

    public long lagAgainst(long headSequence) {
        return ringBufferReader.lagAgainst(headSequence);
    }

    public RealTimeConsumerStats stats() {
        return new RealTimeConsumerStats(
                name,
                ringBufferReader.lastProcessedSequence(),
                ringBufferReader.nextSequence(),
                processedEvents,
                ringBufferReader.missedUpdates());
    }

    @Override
    public void onUpdate(DerivedInstrumentUpdate update) {
        handler.accept(update);
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
            boolean processed = ringBufferReader.readNext();
            if (processed && processingDelayMillis > 0L) {
                ConsumerWaitSupport.delayMillis(processingDelayMillis);
            }
        }
    }
}
