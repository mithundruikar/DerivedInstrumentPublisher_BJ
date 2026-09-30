package com.bj.publisher.consumer;

import com.bj.publisher.entity.DerivedInstrumentUpdate;
import com.bj.publisher.ringbuffer.DerivedUpdateRingBuffer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RealTimeDerivedUpdateConsumerTest {

    @Test
    void shouldForwardUpdateToHandlerAndIncreaseProcessedCount() {
        DerivedUpdateRingBuffer ringBuffer = new DerivedUpdateRingBuffer(8);
        CopyOnWriteArrayList<DerivedInstrumentUpdate> received = new CopyOnWriteArrayList<>();
        try (RealTimeDerivedUpdateConsumer consumer = RealTimeDerivedUpdateConsumer.create(
                "rt-test",
                ringBuffer,
                0L,
                3_600,
                received::add)) {
            DerivedInstrumentUpdate update = new DerivedInstrumentUpdate(
                    1L,
                    1L,
                    1_001L,
                    "EURUSD",
                    1_100_000L,
                    200L,
                    100L,
                    1_100_300L);

            consumer.onUpdate(update);

            assertEquals(1, received.size());
            assertEquals(update, received.get(0));
            assertEquals(1L, consumer.stats().processedEvents());
        }
    }
}
