package com.bj.publisher.component;

import com.bj.publisher.consumer.DurableJournalConsumer;
import com.bj.publisher.consumer.RealTimeDerivedUpdateConsumer;
import com.bj.publisher.journal.JournalWriter;
import com.bj.publisher.ringbuffer.DerivedUpdateRingBuffer;
import com.bj.publisher.service.InstrumentService;
import com.bj.publisher.source.CsvFileSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PublisherFlowComponentTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldProcessCsvAndFanOutToRealtimeConsumersWhileDurableStaysPlaceholder() throws Exception {
        Path csvPath = writeSourceCsv();
        Path journalPath = tempDir.resolve("updates.journal");
        Path durableCheckpoint = tempDir.resolve("durable.checkpoint");
        DerivedUpdateRingBuffer ringBuffer = new DerivedUpdateRingBuffer(32);
        JournalWriter journalWriter = new JournalWriter();

        List<Long> realtimeSequences = new CopyOnWriteArrayList<>();
        List<Long> durableSequences = new CopyOnWriteArrayList<>();

        try (InstrumentService service = new InstrumentService(ringBuffer, journalWriter, 3_600);
             RealTimeDerivedUpdateConsumer serviceRealtimeConsumer = RealTimeDerivedUpdateConsumer.create(
                     "service-realtime",
                     ringBuffer,
                     0L,
                     3_600,
                     update -> realtimeSequences.add(update.derivedSequence()));
             DurableJournalConsumer durableConsumer = DurableJournalConsumer.create(
                     "component-durable",
                     journalPath,
                     durableCheckpoint,
                     0L,
                     3_600,
                     update -> durableSequences.add(update.derivedSequence()));
             CsvFileSource source = new CsvFileSource(csvPath, service, 3_600)) {

            service.registerRealTimeConsumer(serviceRealtimeConsumer);

            serviceRealtimeConsumer.start();
            durableConsumer.start();

            source.run();

            awaitAtLeast("realtime updates", realtimeSequences, 3, Duration.ofSeconds(2));

            assertEquals(List.of(1L, 2L, 3L), realtimeSequences);
            assertTrue(durableSequences.isEmpty());
            assertEquals(1L, source.stats().staleDropped());
            assertEquals(3L, service.stats().derivedEventsPublished());
        }
    }

    private Path writeSourceCsv() throws Exception {
        Path csvPath = tempDir.resolve("source.csv");
        Files.writeString(
                csvPath,
                """
                timestamp_ms,instrument,input_type,value
                1000,EURUSD,base_rate,1.1000
                1001,EURUSD,spread,0.0002
                1002,EURUSD,adjustment,0.0001
                1000,EURUSD,spread,0.0003
                1003,USDJPY,base_rate,149.10
                1004,USDJPY,spread,0.02
                """);
        return csvPath;
    }

    private static void awaitAtLeast(String label, List<?> values, int expected, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (values.size() < expected && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
        }
        assertTrue(values.size() >= expected, "Timed out waiting for " + label);
    }
}
