package com.queueguard.api.ratelimit;

import com.queueguard.shared.RateLimitDecision;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

/**
 * Sliding-window-log rate limiter backed by a single atomic Lua script,
 * so admission decisions are race-free under concurrent requests.
 */
@Service
public class RateLimiterService {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterService.class);

    private final StringRedisTemplate redisTemplate;
    private final RedisScript<List> rateLimiterScript;
    private final long limit;
    private final long windowMillis;

    public RateLimiterService(
            StringRedisTemplate redisTemplate,
            RedisScript<List> slidingWindowRateLimiterScript,
            @Value("${queueguard.rate-limiter.limit:100}") long limit,
            @Value("${queueguard.rate-limiter.window-ms:60000}") long windowMillis) {
        this.redisTemplate = redisTemplate;
        this.rateLimiterScript = slidingWindowRateLimiterScript;
        this.limit = limit;
        this.windowMillis = windowMillis;
    }

    /**
     * Availability is prioritized over strict enforcement: if Redis is unreachable
     * the circuit breaker opens and every request is admitted rather than the
     * rate limiter becoming a single point of failure for the whole API.
     */
    @CircuitBreaker(name = "redisRateLimiter", fallbackMethod = "failOpen")
    public RateLimitDecision checkLimit(String userId) {
        String key = com.queueguard.shared.QueueNames.rateLimitKey(userId);
        long now = System.currentTimeMillis();
        String member = now + "-" + UUID.randomUUID();

        @SuppressWarnings("unchecked")
        List<Long> result = redisTemplate.execute(
                rateLimiterScript,
                List.of(key),
                String.valueOf(now),
                String.valueOf(windowMillis),
                String.valueOf(limit),
                member);

        boolean allowed = result.get(0) == 1L;
        long remaining = result.get(1);
        long resetAfterMillis = result.get(2);

        return allowed
                ? RateLimitDecision.allow(remaining, limit, resetAfterMillis)
                : RateLimitDecision.deny(limit, resetAfterMillis);
    }

    @SuppressWarnings("unused")
    private RateLimitDecision failOpen(String userId, Throwable throwable) {
        log.warn("Rate limiter fail-open for user {}: {}", userId, throwable.toString());
        return RateLimitDecision.allow(limit, limit, windowMillis);
    }
}
