package com.bj.publisher.consumer;

public record DurableConsumerStats(
        String name,
        long lastProcessedSequence,
        long processedEvents,
        long missedUpdates,
        long checkpointOffset) {
}
