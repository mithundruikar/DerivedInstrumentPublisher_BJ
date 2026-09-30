package com.bj.publisher.component;

import com.bj.publisher.consumer.DurableJournalConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableDerivedUpdateConsumerReplayComponentTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldRemainIdleUntilJournalingIsImplemented() throws Exception {
        Path journalPath = tempDir.resolve("updates.journal");
        Path checkpointPath = tempDir.resolve("durable.checkpoint");
        List<Long> values = new CopyOnWriteArrayList<>();
        try (DurableJournalConsumer consumer = DurableJournalConsumer.create(
                "durable-replay-test",
                journalPath,
                checkpointPath,
                0L,
                3_600,
                update -> values.add(update.derivedSequence()))) {
            consumer.start();
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100L));
            assertTrue(values.isEmpty());
            assertEquals(0L, consumer.stats().processedEvents());
            assertEquals(0L, consumer.stats().checkpointOffset());
        }
    }
}
