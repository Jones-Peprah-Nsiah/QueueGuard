package com.queueguard.worker.consumer;

import com.queueguard.shared.QueueNames;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Polls the premium and free streams at a fixed ratio (3 premium : 1 free)
 * so free-tier jobs never fully starve under premium load, without needing
 * a priority-aware primitive that Redis Streams doesn't natively provide.
 */
@Component
public class StreamWorker {

    private static final List<String> SCHEDULE = buildSchedule();

    private final StringRedisTemplate redisTemplate;
    private final JobProcessor jobProcessor;
    private final String consumerName = "worker-" + UUID.randomUUID();
    private final AtomicInteger position = new AtomicInteger(0);

    public StreamWorker(StringRedisTemplate redisTemplate, JobProcessor jobProcessor) {
        this.redisTemplate = redisTemplate;
        this.jobProcessor = jobProcessor;
    }

    private static List<String> buildSchedule() {
        return java.util.stream.Stream.concat(
                        java.util.stream.IntStream.range(0, QueueNames.PREMIUM_TO_FREE_RATIO)
                                .mapToObj(i -> QueueNames.PREMIUM_STREAM),
                        java.util.stream.Stream.of(QueueNames.FREE_STREAM))
                .toList();
    }

    @Scheduled(fixedDelay = 200)
    public void pollOnce() {
        String stream = SCHEDULE.get(position.getAndUpdate(i -> (i + 1) % SCHEDULE.size()));

        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                Consumer.from(QueueNames.CONSUMER_GROUP, consumerName),
                StreamReadOptions.empty().count(1),
                StreamOffset.create(stream, ReadOffset.lastConsumed()));

        if (records == null || records.isEmpty()) {
            return;
        }

        for (MapRecord<String, Object, Object> record : records) {
            jobProcessor.process(stream, record, consumerName);
        }
    }
}
