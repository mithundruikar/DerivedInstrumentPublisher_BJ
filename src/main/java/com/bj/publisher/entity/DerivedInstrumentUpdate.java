package com.bj.publisher.entity;

public record DerivedInstrumentUpdate(
        long derivedSequence,
        long sourceSequence,
        long timestampMs,
        String instrument,
        long baseRateScaled,
        long spreadScaled,
        long adjustmentScaled,
        long derivedValueScaled) {
}
