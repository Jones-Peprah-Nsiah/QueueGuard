package com.queueguard.shared;

import java.time.Instant;
import java.util.UUID;

public record Job(
        String jobId,
        UserTier tier,
        String userId,
        String payload,
        Instant createdAt
) {
    public static Job create(UserTier tier, String userId, String payload) {
        return new Job(UUID.randomUUID().toString(), tier, userId, payload, Instant.now());
    }
}
