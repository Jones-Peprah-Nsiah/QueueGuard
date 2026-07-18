package com.queueguard.api.controller;

import com.queueguard.api.queue.JobEnqueueService;
import com.queueguard.shared.UserTier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobController {

    private final JobEnqueueService jobEnqueueService;

    public JobController(JobEnqueueService jobEnqueueService) {
        this.jobEnqueueService = jobEnqueueService;
    }

    public record EnqueueRequest(String payload) {
    }

    public record EnqueueResponse(String recordId) {
    }

    @PostMapping("/api/jobs")
    public EnqueueResponse enqueue(
            @RequestHeader("X-User-Id") String userId,
            @RequestHeader(value = "X-User-Tier", defaultValue = "FREE") UserTier tier,
            @RequestBody EnqueueRequest request) {
        String recordId = jobEnqueueService.enqueue(tier, userId, request.payload());
        return new EnqueueResponse(recordId);
    }
}
