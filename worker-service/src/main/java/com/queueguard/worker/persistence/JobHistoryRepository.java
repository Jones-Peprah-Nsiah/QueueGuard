package com.queueguard.worker.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

public interface JobHistoryRepository extends JpaRepository<JobHistory, String> {
}
