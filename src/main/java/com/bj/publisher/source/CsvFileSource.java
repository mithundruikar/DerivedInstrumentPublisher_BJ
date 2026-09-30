package com.bj.publisher.source;

import com.bj.publisher.entity.RawInstrumentUpdate;
import com.bj.publisher.entity.ScaledLong;
import com.bj.publisher.metrics.PeriodicStatsLogger;
import com.bj.publisher.entity.InputType;
import com.bj.publisher.service.InstrumentService;
import com.bj.publisher.service.InstrumentServiceResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class CsvFileSource implements SourceAdapter {
    private static final String EXPECTED_HEADER = "timestamp_ms,instrument,input_type,value";

    private static final Logger LOGGER = LogManager.getLogger(CsvFileSource.class);
    private final Path csvPath;
    private final InstrumentService instrumentService;
    private final PeriodicStatsLogger periodicStatsLogger;

    private long currentSequence;
    private long totalEventsRead;
    private long updatesPublished;
    private long malformedDropped;
    private long staleDropped;

    public CsvFileSource(Path csvPath, InstrumentService instrumentService, long statsIntervalSeconds) {
        this.csvPath = csvPath;
        this.instrumentService = instrumentService;
        this.periodicStatsLogger = new PeriodicStatsLogger("csv-file-source", statsIntervalSeconds, () -> LOGGER.info("stats={}", stats()));
    }

    @Override
    public void run() {
        try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
            String header = reader.readLine();
            if (header == null) {
                LOGGER.warn("Source file is empty: {}", csvPath);
                return;
            }
            if (!EXPECTED_HEADER.equals(header.trim())) {
                throw new IllegalArgumentException(
                        "Invalid source header. Expected \"" + EXPECTED_HEADER + "\" but got \"" + header.trim() + "\"");
            }

            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                currentSequence++;
                totalEventsRead++;
                String[] tokens = line.split(",", -1);
                if (tokens.length != 4) {
                    malformedDropped++;
                    LOGGER.warn("Dropped malformed row at line={} row={}", lineNumber, line);
                    continue;
                }

                long timestampMs;
                long valueScaled;
                InputType inputType;
                String instrument;
                try {
                    timestampMs = Long.parseLong(tokens[0].trim());
                    instrument = tokens[1].trim();
                    inputType = InputType.fromWireValue(tokens[2]);
                    valueScaled = ScaledLong.parse(tokens[3].trim());
                } catch (IllegalArgumentException exception) {
                    malformedDropped++;
                    LOGGER.warn("Dropped malformed row at line={} row={} reason={}", lineNumber, line, exception.getMessage());
                    continue;
                }

                RawInstrumentUpdate update = new RawInstrumentUpdate(
                        currentSequence,
                        timestampMs,
                        instrument,
                        inputType,
                        valueScaled);
                InstrumentServiceResult result = instrumentService.onSourceUpdate(update);
                if (result == InstrumentServiceResult.PUBLISHED) {
                    updatesPublished++;
                } else if (result == InstrumentServiceResult.DROPPED_STALE) {
                    staleDropped++;
                }
            }
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to read source file " + csvPath, exception);
        }
    }

    @Override
    public SourceStats stats() {
        return new SourceStats(currentSequence, totalEventsRead, updatesPublished, malformedDropped, staleDropped);
    }

    @Override
    public void close() {
        periodicStatsLogger.close();
    }
}
