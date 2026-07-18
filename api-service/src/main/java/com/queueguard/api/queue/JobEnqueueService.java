package com.queueguard.api.queue;

import com.queueguard.shared.Job;
import com.queueguard.shared.QueueNames;
import com.queueguard.shared.UserTier;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class JobEnqueueService {

    private final StringRedisTemplate redisTemplate;

    public JobEnqueueService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public String enqueue(UserTier tier, String userId, String payload) {
        Job job = Job.create(tier, userId, payload);
        String stream = QueueNames.streamFor(tier);

        MapRecord<String, String, String> record = MapRecord.create(stream, java.util.Map.of(
                "jobId", job.jobId(),
                "userId", job.userId(),
                "tier", job.tier().name(),
                "payload", job.payload(),
                "createdAt", job.createdAt().toString()));

        RecordId recordId = redisTemplate.opsForStream().add(record);
        return recordId != null ? recordId.getValue() : job.jobId();
    }
}
