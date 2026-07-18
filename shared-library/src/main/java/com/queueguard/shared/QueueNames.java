package com.queueguard.shared;

public final class QueueNames {

    public static final String PREMIUM_STREAM = "queueguard:stream:premium";
    public static final String FREE_STREAM = "queueguard:stream:free";
    public static final String CONSUMER_GROUP = "queueguard-workers";

    public static final int PREMIUM_TO_FREE_RATIO = 3;

    public static String streamFor(UserTier tier) {
        return tier == UserTier.PREMIUM ? PREMIUM_STREAM : FREE_STREAM;
    }

    public static String rateLimitKey(String userId) {
        return "queueguard:ratelimit:" + userId;
    }

    private QueueNames() {
    }
}
