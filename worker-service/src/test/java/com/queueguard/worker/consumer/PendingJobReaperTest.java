package com.queueguard.worker.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queueguard.shared.QueueNames;
import com.queueguard.worker.persistence.JobHistoryRepository;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Regression test for the bug found while manually crash-testing the
 * system: PendingJobReaper used to XCLAIM a stale message and stop, never
 * actually processing it, so a job abandoned by a dead worker got stuck
 * forever instead of being completed by a live one. This proves a message
 * claimed-but-never-acked by a simulated dead consumer gets reclaimed and
 * driven to completion once it's been idle past the stale threshold.
 *
 * The scheduled poll loop is left disabled (a poll-interval far longer
 * than the test) so reclaimStaleJobs() is invoked directly for a
 * deterministic trigger point instead of racing a timer.
 */
@Testcontainers
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
            "queueguard.worker.simulated-work-ms=50",
            "queueguard.worker.poll-interval-ms=600000",
            "queueguard.reaper.stale-threshold-ms=500",
            "queueguard.reaper.poll-interval-ms=600000"
        })
class PendingJobReaperTest {

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private PendingJobReaper pendingJobReaper;

    @Autowired
    private JobHistoryRepository jobHistoryRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final String STREAM = QueueNames.PREMIUM_STREAM;

    @AfterEach
    void cleanUp() {
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
        jobHistoryRepository.deleteAll();
    }

    @Test
    void reclaimsAndCompletesAJobAbandonedByADeadConsumer() throws InterruptedException {
        String jobId = UUID.randomUUID().toString();
        createGroupIfAbsent(STREAM);

        redisTemplate
                .opsForStream()
                .add(
                        MapRecord.create(
                                STREAM,
                                Map.of(
                                        "jobId", jobId,
                                        "userId", "crash-test-user",
                                        "tier", "PREMIUM",
                                        "payload", "abandoned-job")));

        // Simulate a worker that claimed the message via XREADGROUP and then
        // died before calling JobProcessor.process() at all — the earliest,
        // worst-case point a crash could happen.
        List<MapRecord<String, Object, Object>> claimedByDeadWorker =
                redisTemplate
                        .opsForStream()
                        .read(
                                Consumer.from(QueueNames.CONSUMER_GROUP, "dead-consumer"),
                                StreamReadOptions.empty().count(1),
                                StreamOffset.create(STREAM, ReadOffset.lastConsumed()));
        assertThat(claimedByDeadWorker).hasSize(1);

        Thread.sleep(700); // past the 500ms stale-threshold

        pendingJobReaper.reclaimStaleJobs();

        await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(
                        () ->
                                assertThat(jobHistoryRepository.findById(jobId))
                                        .isPresent()
                                        .get()
                                        .extracting(h -> h.getStatus())
                                        .isEqualTo("COMPLETED"));

        PendingMessages pending =
                redisTemplate.opsForStream().pending(STREAM, QueueNames.CONSUMER_GROUP, Range.unbounded(), 10);
        assertThat(pending).isEmpty();
    }

    @Test
    void doesNotTouchAJobThatIsStillWithinTheStaleThreshold() {
        String jobId = UUID.randomUUID().toString();
        createGroupIfAbsent(STREAM);

        redisTemplate
                .opsForStream()
                .add(
                        MapRecord.create(
                                STREAM,
                                Map.of(
                                        "jobId", jobId,
                                        "userId", "fresh-user",
                                        "tier", "PREMIUM",
                                        "payload", "in-flight-job")));

        redisTemplate
                .opsForStream()
                .read(
                        Consumer.from(QueueNames.CONSUMER_GROUP, "live-worker"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(STREAM, ReadOffset.lastConsumed()));

        // No sleep: idle time is well under the 500ms stale-threshold, so the
        // reaper must leave it alone rather than stealing work mid-flight.
        pendingJobReaper.reclaimStaleJobs();

        assertThat(jobHistoryRepository.findById(jobId)).isEmpty();

        PendingMessages pending =
                redisTemplate.opsForStream().pending(STREAM, QueueNames.CONSUMER_GROUP, Range.unbounded(), 10);
        assertThat(pending).hasSize(1);
        assertThat(pending.iterator().next().getConsumerName()).isEqualTo("live-worker");
    }

    private void createGroupIfAbsent(String stream) {
        try {
            redisTemplate.opsForStream().createGroup(stream, ReadOffset.from("0"), QueueNames.CONSUMER_GROUP);
        } catch (Exception ignored) {
            // group already exists from a previous test run against this container
        }
    }
}
