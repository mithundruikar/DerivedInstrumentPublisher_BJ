package com.bj.publisher.consumer;

import com.bj.publisher.entity.DerivedInstrumentUpdate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DurableJournalConsumerTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldUpdateStatsWhenOnUpdateIsInvoked() {
        CopyOnWriteArrayList<Long> received = new CopyOnWriteArrayList<>();
        try (DurableJournalConsumer consumer = DurableJournalConsumer.create(
                "durable-unit-test",
                tempDir.resolve("updates.journal"),
                tempDir.resolve("updates.checkpoint"),
                0L,
                3_600,
                update -> received.add(update.derivedSequence()))) {
            consumer.onUpdate(new DerivedInstrumentUpdate(
                    7L,
                    17L,
                    1_007L,
                    "EURUSD",
                    1_100_000L,
                    100L,
                    50L,
                    1_100_150L));

            assertEquals(1, received.size());
            assertEquals(7L, received.get(0));
            assertEquals(1L, consumer.stats().processedEvents());
            assertEquals(7L, consumer.stats().lastProcessedSequence());
        }
    }
}
