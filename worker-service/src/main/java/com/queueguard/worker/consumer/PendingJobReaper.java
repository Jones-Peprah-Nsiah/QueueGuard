package com.queueguard.worker.consumer;

import com.queueguard.shared.QueueNames;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Sweeps each stream's pending-entries list for jobs claimed by a worker that
 * died before ack'ing. XCLAIM hands back the message payload directly, so
 * this reaper drives the reclaimed job through the same JobProcessor path a
 * live worker would use, rather than just relabeling ownership and leaving
 * it stuck (Streams only auto-delivers a message once, via XREADGROUP '>',
 * so a claimed-but-unprocessed message would otherwise never be picked up
 * by anyone else again).
 */
@Component
public class PendingJobReaper {

    private static final Logger log = LoggerFactory.getLogger(PendingJobReaper.class);

    private final StringRedisTemplate redisTemplate;
    private final JobProcessor jobProcessor;
    private final Duration staleThreshold;
    private final String reaperConsumerName = "reaper-" + UUID.randomUUID();

    public PendingJobReaper(
            StringRedisTemplate redisTemplate,
            JobProcessor jobProcessor,
            @Value("${queueguard.reaper.stale-threshold-ms:30000}") long staleThresholdMillis) {
        this.redisTemplate = redisTemplate;
        this.jobProcessor = jobProcessor;
        this.staleThreshold = Duration.ofMillis(staleThresholdMillis);
    }

    @Scheduled(fixedDelayString = "${queueguard.reaper.poll-interval-ms:30000}")
    public void reclaimStaleJobs() {
        reclaim(QueueNames.PREMIUM_STREAM);
        reclaim(QueueNames.FREE_STREAM);
    }

    private void reclaim(String stream) {
        PendingMessages pending;
        try {
            pending = redisTemplate.opsForStream()
                    .pending(stream, QueueNames.CONSUMER_GROUP, Range.unbounded(), 50);
        } catch (Exception e) {
            // Stream or consumer group doesn't exist yet (e.g. no job has ever
            // been enqueued on it, or Redis lost state under the running JVM).
            // One stream being unavailable shouldn't stop the other from being
            // checked, so log and move on rather than letting this propagate.
            log.debug("Could not check pending entries for stream {}: {}", stream, e.toString());
            return;
        }

        if (pending == null || pending.isEmpty()) {
            return;
        }

        for (PendingMessage message : pending) {
            if (message.getElapsedTimeSinceLastDelivery().compareTo(staleThreshold) > 0) {
                log.warn("Reclaiming stale job {} on stream {} (idle {}, previously owned by {})",
                        message.getIdAsString(), stream, message.getElapsedTimeSinceLastDelivery(),
                        message.getConsumerName());

                List<MapRecord<String, Object, Object>> claimed = redisTemplate.opsForStream().claim(
                        stream,
                        QueueNames.CONSUMER_GROUP,
                        reaperConsumerName,
                        XClaimOptions.minIdle(staleThreshold).ids(message.getIdAsString()));

                for (MapRecord<String, Object, Object> record : claimed) {
                    jobProcessor.process(stream, record, reaperConsumerName);
                }
            }
        }
    }
}
