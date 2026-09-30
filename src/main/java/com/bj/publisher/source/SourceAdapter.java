package com.bj.publisher.source;

public interface SourceAdapter extends AutoCloseable {
    void run();

    SourceStats stats();
}
