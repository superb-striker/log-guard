package com.example.logguard.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Persistent log record after PII ((Personally Identifiable Information) redaction.
 * Stored in PostgreSQL via Spring Data JPA.
 */
@Entity
@Table(
    name = "log_entries",
    // Indexes speed up queries
    indexes = {
        @Index(name = "idx_log_level",     columnList = "level"),
        @Index(name = "idx_log_service",   columnList = "service"),
        @Index(name = "idx_log_timestamp", columnList = "log_timestamp"),
        @Index(name = "idx_log_trace_id",  columnList = "trace_id")
    }
)
// Using Lombok annotations to reduce boilerplate : generates these by itself:
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder       // can creates objects like : LogEntry.builder().service("Auth).level("CRITICAL").build()
public class LogEntry {

    @Id   // Primary Key
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(nullable = false, length = 100)
    private String service;

    @Column(nullable = false, length = 20)
    private String level;

    // Redacted message - safe to persist
    @Column(nullable = false, columnDefinition = "TEXT")
    private String message;

    @Column(name = "log_timestamp", nullable = false)
    private Instant timestamp;
    
    // Useful for: tracing request across services
    @Column(name = "trace_id", length = 64)
    private String traceId;   
    
    // Useful in: OpenTelemetry tracing
    @Column(name = "span_id", length = 64)
    private String spanId;

    // Arbitrary metadata stored as JSONB in Postgres
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private Map<String, String> metadata;

    // Whether this entry triggered a CRITICAL alert : Prevents: duplicate alerts, repeated notifications
    @Column(name = "alert_sent", nullable = false)
    @Builder.Default    // defining default value
    private boolean alertSent = false;

    // Timestamp when app received/saved log for observability
    @Column(name = "ingested_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant ingestedAt = Instant.now();

    // Timestamp when PII processing finished
    @Column(name = "processed_at")
    private Instant processedAt;
}
