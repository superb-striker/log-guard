package com.example.logguard.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

/**
 * Inbound Data Transfer Object (DTO) for log entries.
 * All validation is enforced at the REST layer via jakarta.validation.
 */

//Using Lombok annotations to reduce boilerplate : generates these by itself:
@Data   // Generates methods: getters, setters, toString, equals, hashCode
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LogEntryDTO implements Serializable {

    @NotBlank(message = "Service name must not be blank.")
    @Size(max = 100, message = "Service name must not exceed 100 characters.")
    private String service;

    @NotNull(message = "Log level is required.")
    @Pattern(
        regexp = "^(DEBUG|INFO|WARN|ERROR|CRITICAL)$",
        message = "Log level must be one of: DEBUG, INFO, WARN, ERROR, CRITICAL."
    )
    private String level;

    @NotBlank(message = "Message must not be blank.")
    @Size(max = 5000, message = "Message must not exceed 5000 characters.")
    private String message;

    // Can be null -> app fills it in later through resolvedTimestamp
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;

    @Size(max = 64, message = "Trace ID must not exceed 64 characters.")
    private String traceId;

    @Size(max = 64, message = "Span ID must not exceed 64 characters")
    private String spanId;

    /**
     * Arbitrary metadata key-value pairs (e.g. user-agent, region, env).
     * Values are stored as strings to keep the schema flexible.
     */
    private Map<@NotBlank String, @NotBlank String> metadata;

    // Resolved just-in-time if not provided by the caller.
    public Instant resolvedTimestamp() {
        return timestamp != null ? timestamp : Instant.now();
    }
}
