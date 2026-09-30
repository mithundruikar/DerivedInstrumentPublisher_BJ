package com.bj.publisher.consumer;

import com.bj.publisher.entity.DerivedInstrumentUpdate;

public interface DerivedUpdateConsumer {
    void onUpdate(DerivedInstrumentUpdate update);
}
