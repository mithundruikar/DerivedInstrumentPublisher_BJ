package com.bj.publisher.entity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ScaledLongTest {

    @Test
    void shouldParseAndFormatScaledValues() {
        assertEquals(1_100_200L, ScaledLong.parse("1.1002"));
        assertEquals(-100L, ScaledLong.parse("-0.0001"));
        assertEquals("1.1002", ScaledLong.format(1_100_200L));
        assertEquals("-0.0001", ScaledLong.format(-100L));
    }

    @Test
    void shouldRejectValuesWithMoreThanConfiguredScale() {
        assertThrows(IllegalArgumentException.class, () -> ScaledLong.parse("1.0000001"));
    }
}
