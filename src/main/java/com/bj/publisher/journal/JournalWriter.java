package com.bj.publisher.journal;

import com.bj.publisher.entity.DerivedInstrumentUpdate;

/**
 * JournalWriter is responsible for writing DerivedInstrumentUpdate events to a journal.
 * This to be implemented to persist updates for durable consumers to read from.
 * It can be Kafka, custom write ahead log, or any other durable storage mechanism.
 *
 * At least once delivery is guaranteed, but it may deliver the same update multiple times if the consumer is restarted and the journal is not updated in time.
 */
public class JournalWriter {
    public void write(DerivedInstrumentUpdate update) {
        // To be implemented: Write the update to the journal
    }
}
