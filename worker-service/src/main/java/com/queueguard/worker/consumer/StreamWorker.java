package com.queueguard.worker.consumer;

import com.queueguard.shared.QueueNames;
import com.queueguard.worker.persistence.JobHistory;
import com.queueguard.worker.persistence.JobHistoryRepository;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(StreamWorker.class);

    private static final List<String> SCHEDULE = buildSchedule();

    private final StringRedisTemplate redisTemplate;
    private final JobHistoryRepository jobHistoryRepository;
    private final String consumerName = "worker-" + UUID.randomUUID();
    private final AtomicInteger position = new AtomicInteger(0);

    public StreamWorker(StringRedisTemplate redisTemplate, JobHistoryRepository jobHistoryRepository) {
        this.redisTemplate = redisTemplate;
        this.jobHistoryRepository = jobHistoryRepository;
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
            processRecord(stream, record);
        }
    }

    private void processRecord(String stream, MapRecord<String, Object, Object> record) {
        try {
            var fields = record.getValue();
            String jobId = String.valueOf(fields.get("jobId"));
            String userId = String.valueOf(fields.get("userId"));
            String tier = String.valueOf(fields.get("tier"));

            JobHistory history = new JobHistory(jobId, userId, tier, "PROCESSING", Instant.now(), 1);
            jobHistoryRepository.save(history);

            // TODO: replace with actual job execution once there is real work to dispatch.
            history.setStatus("COMPLETED");
            history.setProcessedAt(Instant.now());
            jobHistoryRepository.save(history);

            redisTemplate.opsForStream().acknowledge(stream, QueueNames.CONSUMER_GROUP, record.getId());
        } catch (Exception e) {
            log.error("Failed to process record {} on stream {}, leaving unacked for reaper", record.getId(), stream, e);
        }
    }
}
