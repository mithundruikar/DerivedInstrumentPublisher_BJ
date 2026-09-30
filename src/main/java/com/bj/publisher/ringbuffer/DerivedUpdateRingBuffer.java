package com.bj.publisher.ringbuffer;

import com.bj.publisher.entity.DerivedInstrumentUpdate;

public final class DerivedUpdateRingBuffer {
    private final int capacity;
    private final int mask;
    private final DerivedInstrumentUpdate[] slots;
    private volatile long headSequence;

    public DerivedUpdateRingBuffer(int capacity) {
        if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("Ring buffer capacity must be a positive power of two");
        }
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.slots = new DerivedInstrumentUpdate[capacity];
        this.headSequence = 0L;
    }

    public int capacity() {
        return capacity;
    }

    public long getHeadSequence() {
        return headSequence;
    }

    public long minAvailableSequence(long head) {
        if (head <= 0L) {
            return 1L;
        }
        long min = head - capacity + 1;
        return Math.max(1L, min);
    }

    public void publish(DerivedInstrumentUpdate update) {
        slots[(int) (update.derivedSequence() & mask)] = update;
        headSequence = update.derivedSequence();
    }

    public DerivedInstrumentUpdate copyAt(long sequence) {
        DerivedInstrumentUpdate slot = slots[(int) (sequence & mask)];
        if (slot == null || slot.derivedSequence() != sequence) {
            return null;
        }
        return slot;
    }
}
