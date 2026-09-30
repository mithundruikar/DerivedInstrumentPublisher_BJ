package com.bj.publisher.consumer;

import com.bj.publisher.entity.DerivedInstrumentUpdate;
import com.bj.publisher.ringbuffer.DerivedUpdateRingBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class RingBufferReader {
    private static final Logger LOGGER = LogManager.getLogger(RingBufferReader.class);
    private final String consumerName;
    private final DerivedUpdateRingBuffer ringBuffer;
    private final DerivedUpdateConsumer callbackDerivedUpdateConsumer;

    private volatile long nextSequence;
    private volatile long lastProcessedSequence;
    private volatile long missedUpdates;

    public RingBufferReader(String consumerName, DerivedUpdateRingBuffer ringBuffer, DerivedUpdateConsumer callbackDerivedUpdateConsumer) {
        this.consumerName = consumerName;
        this.ringBuffer = ringBuffer;
        this.callbackDerivedUpdateConsumer = callbackDerivedUpdateConsumer;
        this.nextSequence = ringBuffer.getHeadSequence() + 1;
    }

    public boolean readNext() {
        long headSequence = ringBuffer.getHeadSequence();
        if (headSequence < nextSequence) {
            ConsumerWaitSupport.idle();
            return false;
        }

        long minAvailable = ringBuffer.minAvailableSequence(headSequence);
        if (nextSequence < minAvailable) {
            long missed = minAvailable - nextSequence;
            missedUpdates += missed;
            LOGGER.warn(
                    "Realtime consumer gap name={} missed={} nextSequence={} minAvailable={} head={}",
                    consumerName,
                    missed,
                    nextSequence,
                    minAvailable,
                    headSequence);
            nextSequence = minAvailable;
            return false;
        }

        DerivedInstrumentUpdate update = ringBuffer.copyAt(nextSequence);
        if (update == null) {
            long refreshedHead = ringBuffer.getHeadSequence();
            long refreshedMinAvailable = ringBuffer.minAvailableSequence(refreshedHead);
            if (nextSequence < refreshedMinAvailable) {
                long missed = refreshedMinAvailable - nextSequence;
                missedUpdates += missed;
                LOGGER.warn(
                        "Realtime consumer raced with overwrite name={} missed={} nextSequence={} minAvailable={} head={}",
                        consumerName,
                        missed,
                        nextSequence,
                        refreshedMinAvailable,
                        refreshedHead);
                nextSequence = refreshedMinAvailable;
            }
            return false;
        }

        if (lastProcessedSequence > 0 && update.derivedSequence() > lastProcessedSequence + 1) {
            long missed = update.derivedSequence() - (lastProcessedSequence + 1);
            missedUpdates += missed;
            LOGGER.warn(
                    "Realtime consumer detected sequence gap name={} missed={} expected={} received={}",
                    consumerName,
                    missed,
                    lastProcessedSequence + 1,
                    update.derivedSequence());
        }

        callbackDerivedUpdateConsumer.onUpdate(update);
        lastProcessedSequence = update.derivedSequence();
        nextSequence = lastProcessedSequence + 1;
        return true;
    }

    public long lagAgainst(long headSequence) {
        long lag = headSequence - nextSequence + 1;
        return Math.max(lag, 0L);
    }

    public long nextSequence() {
        return nextSequence;
    }

    public long lastProcessedSequence() {
        return lastProcessedSequence;
    }

    public long missedUpdates() {
        return missedUpdates;
    }
}
