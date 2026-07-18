package com.queueguard.shared;

public record RateLimitDecision(
        boolean allowed,
        long remaining,
        long limit,
        long resetAfterMillis
) {
    public static RateLimitDecision allow(long remaining, long limit, long resetAfterMillis) {
        return new RateLimitDecision(true, remaining, limit, resetAfterMillis);
    }

    public static RateLimitDecision deny(long limit, long resetAfterMillis) {
        return new RateLimitDecision(false, 0, limit, resetAfterMillis);
    }
}
