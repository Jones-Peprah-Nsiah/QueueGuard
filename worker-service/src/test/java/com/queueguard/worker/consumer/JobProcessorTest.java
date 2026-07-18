package com.queueguard.worker.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.queueguard.shared.QueueNames;
import com.queueguard.worker.persistence.JobHistoryRepository;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisConnectionFactory;
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
 * Proves the enqueue -> claim -> process -> ack -> persist round trip
 * against real Redis and Postgres. StreamWorker is mocked out so its own
 * background poll loop can't race this test for the same message.
 */
@Testcontainers
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
            "queueguard.worker.simulated-work-ms=50",
            "queueguard.worker.poll-interval-ms=600000",
            "queueguard.reaper.poll-interval-ms=600000"
        })
class JobProcessorTest {

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
    private JobProcessor jobProcessor;

    @Autowired
    private JobHistoryRepository jobHistoryRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final String STREAM = QueueNames.FREE_STREAM;

    @AfterEach
    void cleanUp() {
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
        jobHistoryRepository.deleteAll();
    }

    @Test
    void processesAJobAndAcksTheStreamMessage() {
        String jobId = UUID.randomUUID().toString();
        createGroupIfAbsent(STREAM);

        redisTemplate
                .opsForStream()
                .add(
                        MapRecord.create(
                                STREAM,
                                Map.of(
                                        "jobId", jobId,
                                        "userId", "test-user",
                                        "tier", "FREE",
                                        "payload", "test-payload")));

        List<MapRecord<String, Object, Object>> records =
                redisTemplate
                        .opsForStream()
                        .read(
                                Consumer.from(QueueNames.CONSUMER_GROUP, "test-consumer"),
                                StreamReadOptions.empty().count(1),
                                StreamOffset.create(STREAM, ReadOffset.lastConsumed()));
        assertThat(records).hasSize(1);

        jobProcessor.process(STREAM, records.get(0), "test-consumer");

        assertThat(jobHistoryRepository.findById(jobId))
                .isPresent()
                .get()
                .satisfies(
                        history -> {
                            assertThat(history.getStatus()).isEqualTo("COMPLETED");
                            assertThat(history.getProcessedAt()).isNotNull();
                        });

        PendingMessages pending =
                redisTemplate.opsForStream().pending(STREAM, QueueNames.CONSUMER_GROUP, Range.unbounded(), 10);
        assertThat(pending).isEmpty();
    }

    private void createGroupIfAbsent(String stream) {
        try {
            redisTemplate.opsForStream().createGroup(stream, ReadOffset.from("0"), QueueNames.CONSUMER_GROUP);
        } catch (Exception ignored) {
            // group already exists from a previous test run against this container
        }
    }
}
