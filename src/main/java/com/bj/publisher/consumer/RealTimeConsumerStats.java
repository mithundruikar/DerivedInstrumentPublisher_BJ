package com.bj.publisher.consumer;

public record RealTimeConsumerStats(
        String name,
        long lastProcessedSequence,
        long nextSequence,
        long processedEvents,
        long missedUpdates) {
}
