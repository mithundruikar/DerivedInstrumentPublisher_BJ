package com.bj.publisher.service;

import com.bj.publisher.consumer.RealTimeDerivedUpdateConsumer;
import com.bj.publisher.entity.DerivedInstrumentUpdate;
import com.bj.publisher.entity.RawInstrumentUpdate;
import com.bj.publisher.journal.JournalWriter;
import com.bj.publisher.metrics.PeriodicStatsLogger;
import com.bj.publisher.ringbuffer.DerivedUpdateRingBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

public final class InstrumentService implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(InstrumentService.class);
    private final DerivedUpdateRingBuffer ringBuffer;
    private final PeriodicStatsLogger periodicStatsLogger;
    private final Map<String, InstrumentState> stateByInstrument;
    private final List<RealTimeDerivedUpdateConsumer> realtimeConsumers;

    // Single-writer only field; no volatile needed.
    private long nextDerivedSequence;
    // Written by source writer thread, read by stats/logging threads.
    private volatile long sourceEventsReceived;
    private volatile long derivedEventsPublished;
    private volatile long staleEventsDropped;
    private final AtomicReference<Thread> writerThread;
    private final JournalWriter journalWriter;

    public InstrumentService(DerivedUpdateRingBuffer ringBuffer, JournalWriter journalWriter, long statsIntervalSeconds) {
        this.ringBuffer = ringBuffer;
        this.journalWriter = journalWriter;
        this.periodicStatsLogger = new PeriodicStatsLogger("instrument-service", statsIntervalSeconds, () -> LOGGER.info("stats={}", stats()));
        this.stateByInstrument = new ConcurrentHashMap<>();
        this.realtimeConsumers = new CopyOnWriteArrayList<>();
        this.nextDerivedSequence = 1L;
        this.writerThread = new AtomicReference<>();
    }

    public InstrumentServiceResult onSourceUpdate(
            RawInstrumentUpdate sourceUpdate) {
        assertSingleWriter();
        sourceEventsReceived++;
        InstrumentState state = stateByInstrument.computeIfAbsent(sourceUpdate.instrument(), InstrumentState::new);
        if (state.isStale(sourceUpdate.timestampMs())) {
            staleEventsDropped++;
            LOGGER.warn(
                    "Dropped stale source update instrument={} sourceSequence={} timestampMs={} lastTimestampMs={}",
                    sourceUpdate.instrument(),
                    sourceUpdate.sourceSequence(),
                    sourceUpdate.timestampMs(),
                    state.lastUpdatedTimestampMs());
            return InstrumentServiceResult.DROPPED_STALE;
        }

        state.apply(
                sourceUpdate.sourceSequence(),
                sourceUpdate.timestampMs(),
                sourceUpdate.inputType(),
                sourceUpdate.valueScaled());
        if (!state.readyToPublish()) {
            return InstrumentServiceResult.APPLIED_NOT_READY;
        }

        long derivedSequence = nextDerivedSequence++;
        long derivedValueScaled = state.derivedValueScaled();
        DerivedInstrumentUpdate derivedUpdate = new DerivedInstrumentUpdate(
                derivedSequence,
                state.lastAppliedSequence(),
                state.lastUpdatedTimestampMs(),
                state.instrument(),
                state.baseRateScaled(),
                state.spreadScaled(),
                state.adjustmentScaled(),
                derivedValueScaled);

        ringBuffer.publish(derivedUpdate);
        journalWriter.write(derivedUpdate);

        derivedEventsPublished++;
        return InstrumentServiceResult.PUBLISHED;
    }

    public void registerRealTimeConsumer(RealTimeDerivedUpdateConsumer consumer) {
        realtimeConsumers.add(consumer);
    }

    public InstrumentServiceStats stats() {
        long headSequence = ringBuffer.getHeadSequence();
        long occupancy = Math.min(headSequence, ringBuffer.capacity());
        long slowestLag = 0L;
        for (RealTimeDerivedUpdateConsumer consumer : realtimeConsumers) {
            long lag = consumer.lagAgainst(headSequence);
            slowestLag = Math.max(slowestLag, lag);
        }
        return new InstrumentServiceStats(
                headSequence,
                sourceEventsReceived,
                derivedEventsPublished,
                staleEventsDropped,
                stateByInstrument.size(),
                ringBuffer.capacity(),
                occupancy,
                realtimeConsumers.size(),
                slowestLag);
    }

    @Override
    public void close() {
        periodicStatsLogger.close();
    }

    private void assertSingleWriter() {
        Thread currentThread = Thread.currentThread();
        Thread registeredWriter = writerThread.get();
        if (registeredWriter == null && writerThread.compareAndSet(null, currentThread)) {
            return;
        }
        registeredWriter = writerThread.get();
        if (registeredWriter != currentThread) {
            throw new IllegalStateException(
                    "InstrumentService supports a single writer thread. Registered writer="
                            + registeredWriter.getName()
                            + " current writer="
                            + currentThread.getName());
        }
    }
}
