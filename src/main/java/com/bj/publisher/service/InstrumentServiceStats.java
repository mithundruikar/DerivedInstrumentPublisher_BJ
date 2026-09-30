package com.bj.publisher.service;

public record InstrumentServiceStats(
        long currentDerivedSequence,
        long sourceEventsReceived,
        long derivedEventsPublished,
        long staleEventsDropped,
        int instrumentCount,
        int ringBufferCapacity,
        long ringBufferOccupancy,
        int consumerCount,
        long slowestConsumerLag) {
}
