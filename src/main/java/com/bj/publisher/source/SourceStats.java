package com.bj.publisher.source;

public record SourceStats(
        long currentSequence,
        long totalEventsRead,
        long updatesPublished,
        long malformedDropped,
        long staleDropped) {
}
