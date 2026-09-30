package com.bj.publisher.ringbuffer;

import com.bj.publisher.entity.DerivedInstrumentUpdate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class DerivedUpdateRingBufferTest {

    @Test
    void shouldOverwriteOldestEntriesWhenCapacityIsExceeded() {
        DerivedUpdateRingBuffer ringBuffer = new DerivedUpdateRingBuffer(4);

        for (int sequence = 1; sequence <= 6; sequence++) {
            ringBuffer.publish(new DerivedInstrumentUpdate(
                    sequence,
                    sequence,
                    1_000L + sequence,
                    "INSTR-" + sequence,
                    1_000_000L,
                    100_000L,
                    0L,
                    1_100_000L));
        }

        assertEquals(6L, ringBuffer.getHeadSequence());
        assertEquals(3L, ringBuffer.minAvailableSequence(ringBuffer.getHeadSequence()));

        assertNull(ringBuffer.copyAt(2L));

        DerivedInstrumentUpdate update = ringBuffer.copyAt(3L);
        assertNotNull(update);
        assertEquals(3L, update.derivedSequence());
        assertEquals("INSTR-3", update.instrument());
    }
}
