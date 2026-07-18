package com.queueguard.worker.consumer;

import com.queueguard.shared.QueueNames;
import java.time.Duration;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Sweeps each stream's pending-entries list for jobs claimed by a worker
 * that died before ack'ing, and reassigns them to this reaper's consumer
 * so a live worker picks them back up on its next XREADGROUP.
 */
@Component
public class PendingJobReaper {

    private static final Logger log = LoggerFactory.getLogger(PendingJobReaper.class);
    private static final Duration STALE_THRESHOLD = Duration.ofSeconds(30);

    private final StringRedisTemplate redisTemplate;
    private final String reaperConsumerName = "reaper-" + UUID.randomUUID();

    public PendingJobReaper(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Scheduled(fixedDelay = 30000)
    public void reclaimStaleJobs() {
        reclaim(QueueNames.PREMIUM_STREAM);
        reclaim(QueueNames.FREE_STREAM);
    }

    private void reclaim(String stream) {
        PendingMessages pending = redisTemplate.opsForStream()
                .pending(stream, QueueNames.CONSUMER_GROUP, Range.unbounded(), 50);

        if (pending == null || pending.isEmpty()) {
            return;
        }

        for (PendingMessage message : pending) {
            if (message.getElapsedTimeSinceLastDelivery().compareTo(STALE_THRESHOLD) > 0) {
                redisTemplate.opsForStream().claim(
                        stream,
                        QueueNames.CONSUMER_GROUP,
                        reaperConsumerName,
                        XClaimOptions.minIdle(STALE_THRESHOLD).ids(message.getIdAsString()));
                log.warn("Reclaimed stale job {} on stream {} (idle {})",
                        message.getIdAsString(), stream, message.getElapsedTimeSinceLastDelivery());
            }
        }
    }
}
