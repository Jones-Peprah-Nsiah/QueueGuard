package com.queueguard.worker.consumer;

import com.queueguard.shared.QueueNames;
import com.queueguard.worker.persistence.JobHistory;
import com.queueguard.worker.persistence.JobHistoryRepository;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Shared by the normal poll loop and the reaper: whoever ends up holding a
 * message (via XREADGROUP or via XCLAIM) drives it through the same
 * process-then-ack path, since Streams delivers a claimed message's payload
 * directly rather than requiring a separate hand-off to a live worker.
 */
@Component
public class JobProcessor {

    private static final Logger log = LoggerFactory.getLogger(JobProcessor.class);
    static final long SIMULATED_WORK_MILLIS = 15000;

    private final StringRedisTemplate redisTemplate;
    private final JobHistoryRepository jobHistoryRepository;

    public JobProcessor(StringRedisTemplate redisTemplate, JobHistoryRepository jobHistoryRepository) {
        this.redisTemplate = redisTemplate;
        this.jobHistoryRepository = jobHistoryRepository;
    }

    public void process(String stream, MapRecord<String, Object, Object> record, String ownerLabel) {
        try {
            var fields = record.getValue();
            String jobId = String.valueOf(fields.get("jobId"));
            String userId = String.valueOf(fields.get("userId"));
            String tier = String.valueOf(fields.get("tier"));

            log.info("[{}] claimed job {} ({}) from {}", ownerLabel, jobId, userId, stream);

            JobHistory history = jobHistoryRepository.findById(jobId).orElse(null);
            if (history == null) {
                history = new JobHistory(jobId, userId, tier, "PROCESSING", Instant.now(), 1);
            } else {
                history.setStatus("PROCESSING");
                history.setAttempts(history.getAttempts() + 1);
            }
            jobHistoryRepository.save(history);

            // TODO: replace with actual job execution once there is real work to dispatch.
            // Simulated delay stands in for that work so a worker crash/kill during
            // processing leaves a genuinely reclaimable pending entry to demonstrate.
            Thread.sleep(SIMULATED_WORK_MILLIS);

            history.setStatus("COMPLETED");
            history.setProcessedAt(Instant.now());
            jobHistoryRepository.save(history);

            redisTemplate.opsForStream().acknowledge(stream, QueueNames.CONSUMER_GROUP, record.getId());
            log.info("[{}] completed job {}", ownerLabel, jobId);
        } catch (Exception e) {
            log.error("Failed to process record {} on stream {}, leaving unacked for reaper", record.getId(), stream, e);
        }
    }
}
