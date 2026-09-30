package com.bj.publisher;

import com.bj.publisher.consumer.RealTimeDerivedUpdateConsumer;
import com.bj.publisher.entity.ScaledLong;
import com.bj.publisher.journal.JournalWriter;
import com.bj.publisher.ringbuffer.DerivedUpdateRingBuffer;
import com.bj.publisher.service.InstrumentService;
import com.bj.publisher.source.CsvFileSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class Main {
    private static final Logger LOGGER = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            LOGGER.info("Usage: java Main <path-to-source.csv>");
            return;
        }

        Path sourcePath = Path.of(args[0]);
        if (!Files.exists(sourcePath)) {
            LOGGER.warn("Source file not found: {}", sourcePath);
            return;
        }

        Path runtimeDir = Path.of("runtime-data");
        Files.createDirectories(runtimeDir);

        DerivedUpdateRingBuffer ringBuffer = new DerivedUpdateRingBuffer(1024 * 16);
        JournalWriter journalWriter = new JournalWriter();
        AtomicBoolean shutdownRequested = new AtomicBoolean(false);
        Thread shutdownHook = new Thread(() -> {
            shutdownRequested.set(true);
            LOGGER.info("Shutdown requested. Stopping main loop.");
        }, "shutdown-hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try (InstrumentService instrumentService = new InstrumentService(ringBuffer, journalWriter, 5);
             RealTimeDerivedUpdateConsumer fastConsumer = RealTimeDerivedUpdateConsumer.create(
                     "realtime-fast",
                     ringBuffer,
                     10L,
                     5,
                     update -> {
                         LOGGER.info(
                                 "Fast Consumer seq={} instrument={} derived={}",
                                 update.derivedSequence(),
                                 update.instrument(),
                                 ScaledLong.format(update.derivedValueScaled()));
                     });


             RealTimeDerivedUpdateConsumer slowConsumer = RealTimeDerivedUpdateConsumer.create(
                     "realtime-slow",
                     ringBuffer,
                     1000L,
                     5,
                     update -> {
                         // Simulates slower UI layer.
                         LOGGER.info(
                                 "Slow Consumer seq={} instrument={} derived={}",
                                 update.derivedSequence(),
                                 update.instrument(),
                                 ScaledLong.format(update.derivedValueScaled()));
                     });

             CsvFileSource fileSource = new CsvFileSource(sourcePath, instrumentService, 5)) {

            instrumentService.registerRealTimeConsumer(fastConsumer);
            instrumentService.registerRealTimeConsumer(slowConsumer);

            fastConsumer.start();
            slowConsumer.start();

            fileSource.run();

            long targetSequence = instrumentService.stats().currentDerivedSequence();
            LOGGER.info("Source finished at sequence={}. Waiting for consumers to catch up (Ctrl+C to stop).", targetSequence);
            waitForConsumersToCatchUp(targetSequence, shutdownRequested, fastConsumer, slowConsumer);

            LOGGER.info("Final source stats: {}", fileSource.stats());
            LOGGER.info("Final service stats: {}", instrumentService.stats());
            LOGGER.info("Final fast consumer stats: {}", fastConsumer.stats());
            LOGGER.info("Final slow consumer stats: {}", slowConsumer.stats());
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // Shutdown is already in progress.
            }
        }
    }

    private static void waitForConsumersToCatchUp(
            long targetSequence,
            AtomicBoolean shutdownRequested,
            RealTimeDerivedUpdateConsumer... consumers) {
        while (!shutdownRequested.get()) {
            boolean allCaughtUp = true;
            for (RealTimeDerivedUpdateConsumer consumer : consumers) {
                if (consumer.lagAgainst(targetSequence) > 0L) {
                    allCaughtUp = false;
                    break;
                }
            }
            if (allCaughtUp) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
        }
    }
}
