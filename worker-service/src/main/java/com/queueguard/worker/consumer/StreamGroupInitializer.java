package com.queueguard.worker.consumer;

import com.queueguard.shared.QueueNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Ensures the consumer group exists on both priority streams before any
 * worker starts polling. XGROUP CREATE is idempotent from our side: a
 * BUSYGROUP error on restart just means the group is already there.
 */
@Component
public class StreamGroupInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(StreamGroupInitializer.class);

    private final StringRedisTemplate redisTemplate;

    public StreamGroupInitializer(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void run(ApplicationArguments args) {
        createGroupIfAbsent(QueueNames.PREMIUM_STREAM);
        createGroupIfAbsent(QueueNames.FREE_STREAM);
    }

    private void createGroupIfAbsent(String stream) {
        try {
            redisTemplate.opsForStream().createGroup(stream, ReadOffset.from("0"), QueueNames.CONSUMER_GROUP);
            log.info("Created consumer group {} on stream {}", QueueNames.CONSUMER_GROUP, stream);
        } catch (Exception e) {
            log.debug("Consumer group {} already exists on stream {}", QueueNames.CONSUMER_GROUP, stream);
        }
    }
}
