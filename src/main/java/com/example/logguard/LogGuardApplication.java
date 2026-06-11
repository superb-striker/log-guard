package com.example.logguard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Log-guard - Production-grade event-driven log pipeline.
 *
 * Architecture overview:
 *  ┌─────────────┐    AMQP      ┌──────────────────┐    JPA     ┌──────────────┐
 *  │ REST Ingest │ ──────────>  │  Worker Listener │ ────────>  │  PostgreSQL  │
 *  └─────────────┘              └──────────────────┘            └──────────────┘
 *                                        │  CRITICAL?
 *                                        ▼
 *                               ┌──────────────────┐
 *                               │  Alert Service   │  ──>  Webhook (w/ @Retryable)
 *                               └──────────────────┘
 *                                        │  failure
 *                                        ▼
 *                               ┌──────────────────┐
 *                               │  Dead Letter Q   │  (DLX -> DLQ)
 *                               └──────────────────┘
 */
@SpringBootApplication
@EnableScheduling
@EnableRetry
@EnableAsync
public class LogGuardApplication {

    public static void main(String[] args) {
        SpringApplication.run(LogGuardApplication.class, args);
    }
}
