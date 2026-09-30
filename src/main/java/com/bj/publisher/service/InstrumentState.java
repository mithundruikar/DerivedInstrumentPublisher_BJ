package com.bj.publisher.service;

import com.bj.publisher.entity.InputType;

final class InstrumentState {
    private final String instrument;
    private boolean hasBaseRate;
    private boolean hasSpread;
    private long baseRateScaled;
    private long spreadScaled;
    private long adjustmentScaled;
    private long lastUpdatedTimestampMs;
    private long lastAppliedSequence;

    InstrumentState(String instrument) {
        this.instrument = instrument;
        this.lastUpdatedTimestampMs = Long.MIN_VALUE;
        this.adjustmentScaled = 0L;
    }

    String instrument() {
        return instrument;
    }

    boolean isStale(long timestampMs) {
        return timestampMs < lastUpdatedTimestampMs;
    }

    void apply(long sourceSequence, long timestampMs, InputType inputType, long valueScaled) {
        switch (inputType) {
            case BASE_RATE -> {
                hasBaseRate = true;
                baseRateScaled = valueScaled;
            }
            case SPREAD -> {
                hasSpread = true;
                spreadScaled = valueScaled;
            }
            case ADJUSTMENT -> adjustmentScaled = valueScaled;
        }
        lastUpdatedTimestampMs = timestampMs;
        lastAppliedSequence = sourceSequence;
    }

    boolean readyToPublish() {
        return hasBaseRate && hasSpread;
    }

    long lastUpdatedTimestampMs() {
        return lastUpdatedTimestampMs;
    }

    long lastAppliedSequence() {
        return lastAppliedSequence;
    }

    long baseRateScaled() {
        return baseRateScaled;
    }

    long spreadScaled() {
        return spreadScaled;
    }

    long adjustmentScaled() {
        return adjustmentScaled;
    }

    long derivedValueScaled() {
        return Math.addExact(Math.addExact(baseRateScaled, spreadScaled), adjustmentScaled);
    }
}
