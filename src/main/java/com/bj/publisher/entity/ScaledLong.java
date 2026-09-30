package com.bj.publisher.entity;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class ScaledLong {
    public static final int SCALE = 6;

    private ScaledLong() {
    }

    public static long parse(String rawValue) {
        BigDecimal decimal = new BigDecimal(rawValue.trim());
        BigDecimal scaled = decimal.movePointRight(SCALE);
        if (scaled.stripTrailingZeros().scale() > 0) {
            throw new IllegalArgumentException("Value has more than " + SCALE + " decimal places: " + rawValue);
        }
        return scaled.longValueExact();
    }

    public static String format(long scaledValue) {
        BigDecimal decimal = BigDecimal.valueOf(scaledValue, SCALE);
        return decimal.setScale(SCALE, RoundingMode.UNNECESSARY).stripTrailingZeros().toPlainString();
    }
}
