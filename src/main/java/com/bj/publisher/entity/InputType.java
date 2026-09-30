package com.bj.publisher.entity;

public enum InputType {
    BASE_RATE,
    SPREAD,
    ADJUSTMENT;

    public static InputType fromWireValue(String rawValue) {
        return switch (rawValue.trim().toLowerCase()) {
            case "base_rate" -> BASE_RATE;
            case "spread" -> SPREAD;
            case "adjustment" -> ADJUSTMENT;
            default -> throw new IllegalArgumentException("Unknown input_type: " + rawValue);
        };
    }
}
