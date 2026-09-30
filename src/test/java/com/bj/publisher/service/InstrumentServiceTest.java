package com.bj.publisher.service;

import com.bj.publisher.entity.DerivedInstrumentUpdate;
import com.bj.publisher.entity.InputType;
import com.bj.publisher.entity.RawInstrumentUpdate;
import com.bj.publisher.journal.JournalWriter;
import com.bj.publisher.ringbuffer.DerivedUpdateRingBuffer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class InstrumentServiceTest {

    @Test
    void shouldPublishOnlyAfterBaseRateAndSpreadArePresent() {
        DerivedUpdateRingBuffer ringBuffer = new DerivedUpdateRingBuffer(16);
        JournalWriter journalWriter = new JournalWriter();

        try (InstrumentService service = new InstrumentService(ringBuffer, journalWriter, 3_600)) {

            assertEquals(
                    InstrumentServiceResult.APPLIED_NOT_READY,
                    service.onSourceUpdate(new RawInstrumentUpdate(1L, 1_000L, "EURUSD", InputType.BASE_RATE, 1_100_000L)));

            assertEquals(
                    InstrumentServiceResult.PUBLISHED,
                    service.onSourceUpdate(new RawInstrumentUpdate(2L, 1_001L, "EURUSD", InputType.SPREAD, 200L)));

            assertEquals(
                    InstrumentServiceResult.PUBLISHED,
                    service.onSourceUpdate(new RawInstrumentUpdate(3L, 1_002L, "EURUSD", InputType.ADJUSTMENT, 100L)));

            assertEquals(
                    InstrumentServiceResult.DROPPED_STALE,
                    service.onSourceUpdate(new RawInstrumentUpdate(4L, 999L, "EURUSD", InputType.SPREAD, 300L)));

            DerivedInstrumentUpdate first = ringBuffer.copyAt(1L);
            DerivedInstrumentUpdate second = ringBuffer.copyAt(2L);
            assertNotNull(first);
            assertNotNull(second);

            assertEquals(1_100_200L, first.derivedValueScaled());
            assertEquals(1_100_300L, second.derivedValueScaled());
            assertEquals(3L, second.sourceSequence());
            assertEquals(2L, service.stats().currentDerivedSequence());
            assertEquals(1L, service.stats().staleEventsDropped());
        }
    }

    @Test
    void shouldRejectSecondWriterThread() throws Exception {
        DerivedUpdateRingBuffer ringBuffer = new DerivedUpdateRingBuffer(16);
        JournalWriter journalWriter = new JournalWriter();
        try (InstrumentService service = new InstrumentService(ringBuffer, journalWriter, 3_600)) {
            service.onSourceUpdate(new RawInstrumentUpdate(1L, 1_000L, "EURUSD", InputType.BASE_RATE, 1_100_000L));

            AtomicReference<Throwable> thrown = new AtomicReference<>();
            Thread otherWriter = new Thread(() -> {
                try {
                    service.onSourceUpdate(new RawInstrumentUpdate(2L, 1_001L, "EURUSD", InputType.SPREAD, 200L));
                } catch (Throwable throwable) {
                    thrown.set(throwable);
                }
            }, "other-writer");
            otherWriter.start();
            otherWriter.join();

            assertInstanceOf(IllegalStateException.class, thrown.get());
        }
    }
}
