package com.bj.publisher.consumer;

import com.bj.publisher.entity.DerivedInstrumentUpdate;
import com.bj.publisher.ringbuffer.DerivedUpdateRingBuffer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RingBufferReaderTest {

    @Test
    void shouldReadPublishedUpdateAndAdvanceReaderState() {
        DerivedUpdateRingBuffer ringBuffer = new DerivedUpdateRingBuffer(8);
        CopyOnWriteArrayList<Long> receivedSequences = new CopyOnWriteArrayList<>();
        RingBufferReader reader = new RingBufferReader("reader-test", ringBuffer, update -> receivedSequences.add(update.derivedSequence()));

        ringBuffer.publish(update(1L, "EURUSD"));

        assertTrue(reader.readNext());
        assertEquals(1L, reader.lastProcessedSequence());
        assertEquals(2L, reader.nextSequence());
        assertEquals(0L, reader.missedUpdates());
        assertEquals(0L, reader.lagAgainst(ringBuffer.getHeadSequence()));
        assertEquals(1, receivedSequences.size());
        assertEquals(1L, receivedSequences.get(0));
    }

    @Test
    void shouldTrackGapWhenReaderFallsBehindRingBufferCapacity() {
        DerivedUpdateRingBuffer ringBuffer = new DerivedUpdateRingBuffer(4);
        CopyOnWriteArrayList<Long> receivedSequences = new CopyOnWriteArrayList<>();
        RingBufferReader reader = new RingBufferReader("reader-gap-test", ringBuffer, update -> receivedSequences.add(update.derivedSequence()));

        for (long sequence = 1L; sequence <= 6L; sequence++) {
            ringBuffer.publish(update(sequence, "INSTR"));
        }

        assertFalse(reader.readNext());
        assertEquals(2L, reader.missedUpdates());
        assertEquals(3L, reader.nextSequence());

        assertTrue(reader.readNext());
        assertEquals(3L, reader.lastProcessedSequence());
        assertEquals(4L, reader.nextSequence());
        assertEquals(1, receivedSequences.size());
        assertEquals(3L, receivedSequences.get(0));
    }

    private static DerivedInstrumentUpdate update(long sequence, String instrument) {
        return new DerivedInstrumentUpdate(
                sequence,
                sequence,
                1_000L + sequence,
                instrument,
                1_000_000L,
                100L,
                0L,
                1_000_100L);
    }
}
