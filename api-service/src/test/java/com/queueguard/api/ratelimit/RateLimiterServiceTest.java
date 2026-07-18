package com.queueguard.api.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import com.queueguard.shared.RateLimitDecision;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Exercises the sliding-window-log rate limiter against a real Redis
 * instance instead of mocking it: the whole point of the design (an atomic
 * Lua script over MULTI/WATCH) is a claim about behavior under concurrency
 * that a mock can't verify.
 */
@Testcontainers
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
            "queueguard.rate-limiter.limit=10",
            "queueguard.rate-limiter.window-ms=2000"
        })
class RateLimiterServiceTest {

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    private RateLimiterService rateLimiterService;

    @Autowired
    private RedisConnectionFactory redisConnectionFactory;

    @AfterEach
    void flushRedis() {
        redisConnectionFactory.getConnection().serverCommands().flushAll();
    }

    @Test
    void admitsRequestsUpToTheLimit() {
        String userId = "user-under-limit";

        for (int i = 0; i < 10; i++) {
            assertThat(rateLimiterService.checkLimit(userId).allowed()).isTrue();
        }
    }

    @Test
    void deniesRequestsOnceLimitIsExceeded() {
        String userId = "user-over-limit";

        for (int i = 0; i < 10; i++) {
            assertThat(rateLimiterService.checkLimit(userId).allowed()).isTrue();
        }

        RateLimitDecision denied = rateLimiterService.checkLimit(userId);

        assertThat(denied.allowed()).isFalse();
        assertThat(denied.resetAfterMillis()).isGreaterThan(0);
    }

    @Test
    void slidingWindowAdmitsAgainOnceOldEntriesExpire() throws InterruptedException {
        String userId = "user-sliding-window";

        for (int i = 0; i < 10; i++) {
            assertThat(rateLimiterService.checkLimit(userId).allowed()).isTrue();
        }
        assertThat(rateLimiterService.checkLimit(userId).allowed()).isFalse();

        Thread.sleep(2100); // past the 2s window

        assertThat(rateLimiterService.checkLimit(userId).allowed()).isTrue();
    }

    @Test
    void exactlyTheLimitIsAdmittedUnderConcurrentLoad() throws Exception {
        String userId = "user-concurrent";
        int threadCount = 50; // far more than the limit of 10, to force real contention

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        try {
            List<Callable<Boolean>> tasks =
                    IntStream.range(0, threadCount)
                            .<Callable<Boolean>>mapToObj(
                                    i -> () -> rateLimiterService.checkLimit(userId).allowed())
                            .toList();

            List<Future<Boolean>> results = pool.invokeAll(tasks);

            long admittedCount =
                    results.stream()
                            .map(RateLimiterServiceTest::getUnchecked)
                            .filter(Boolean::booleanValue)
                            .count();

            // The core atomicity proof: a naive check-then-increment (ZCARD then
            // ZADD as two separate round trips) would let concurrent threads race
            // past the check before any of them writes, over-admitting past the
            // limit. The Lua script makes the whole decision a single atomic op.
            assertThat(admittedCount).isEqualTo(10);
        } finally {
            pool.shutdown();
        }
    }

    private static <T> T getUnchecked(Future<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
