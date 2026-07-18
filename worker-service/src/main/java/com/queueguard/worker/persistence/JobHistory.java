package com.queueguard.worker.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "job_history")
public class JobHistory {

    @Id
    @Column(name = "job_id")
    private String jobId;

    @Column(name = "user_id", nullable = false)
    private String userId;

    @Column(name = "tier", nullable = false)
    private String tier;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    @Column(name = "attempts", nullable = false)
    private int attempts;

    protected JobHistory() {
    }

    public JobHistory(String jobId, String userId, String tier, String status, Instant createdAt, int attempts) {
        this.jobId = jobId;
        this.userId = userId;
        this.tier = tier;
        this.status = status;
        this.createdAt = createdAt;
        this.attempts = attempts;
    }

    public String getJobId() {
        return jobId;
    }

    public String getUserId() {
        return userId;
    }

    public String getTier() {
        return tier;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(Instant processedAt) {
        this.processedAt = processedAt;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }
}
