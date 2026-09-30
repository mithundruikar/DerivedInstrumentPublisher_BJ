package com.bj.publisher.entity;

public record RawInstrumentUpdate(
        long sourceSequence,
        long timestampMs,
        String instrument,
        InputType inputType,
        long valueScaled) {
}
