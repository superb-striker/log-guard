package com.example.logguard.service;

import com.example.logguard.dto.LogEntryDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.time.Instant;
import java.util.Map;

/**
 * AlertService - sends asynchronous webhook notifications for CRITICAL logs.
 *
 * Resilience strategy:
 *  • @Async          - non-blocking; doesn't stall the worker thread
 *  • @Retryable      - exponential backoff: 1s -> 2s -> 4s (max 3 attempts)
 *  • @Recover        - graceful fallback logged when all retries are exhausted
 *
 * The separation of @Async and @Retryable requires that this service is called
 * from another Spring bean (proxy boundary), not from within the same class.
 */
@Service
public class AlertService {

    private static final Logger log = LoggerFactory.getLogger(AlertService.class);

    private final WebClient webClient;

    @Value("${guard.webhook.url}")
    private String webhookUrl;

    // App may have multiple WebClients. @Qualifider ensures: right timeout config and correct webhook client
    public AlertService(@Qualifier("webhookWebClient") WebClient webClient) {
        this.webClient = webClient;
    }

    /**
     * Dispatches a CRITICAL alert to the configured webhook endpoint.
     *
     * Retry policy:
     *  - maxAttempts = 3
     *  - initialDelay = 1000 ms
     *  - multiplier   = 2.0  (exponential: 1s, 2s, 4s)
     *  - Retries on IOException and WebClientResponseException (5xx)
     *
     * @param dto the original (pre-redaction) DTO for alert payload construction
     */
    @Async
    @Retryable(
        retryFor  = { Exception.class },
        noRetryFor = { WebClientResponseException.BadRequest.class,
                       WebClientResponseException.Unauthorized.class },
        maxAttempts = 3,
        backoff = @Backoff(
            delay      = 1000,
            multiplier = 2.0,
            maxDelay   = 30_000,
            random     = true    // jitter to prevent thundering herd
        )
    )
    public void sendCriticalAlert(LogEntryDTO dto) {
        log.warn("!!!! CRITICAL log detected - dispatching alert for service [{}]", dto.getService());

        Map<String, Object> payload = buildPayload(dto);

        webClient.post()
            .uri(webhookUrl)
            .bodyValue(payload)
            .retrieve()                // execute request - get response
            .bodyToMono(String.class)  // response body as string
            .doOnSuccess(resp -> log.info("Webhook alert delivered for service [{}]", dto.getService()))
            .doOnError(err  -> log.error("Webhook alert delivery failed: {}", err.getMessage()))
            .block(); // block is acceptable inside @Async thread
    }

    /**
     * Recovery method - invoked after all @Retryable attempts are exhausted.
     * At this point we log and optionally could push to a fallback channel (PagerDuty, SNS, etc.)
     */
    @Recover
    public void recoverAlert(Exception ex, LogEntryDTO dto) {
        log.error(
            "ALERT DELIVERY FAILED after all retries - service=[{}] level=[{}] error=[{}]",
            dto.getService(), dto.getLevel(), ex.getMessage()
        );
        // TODO: publish failed alerts to a fallback queue / persistence store for later retry and incident auditing
    }

    // Private method

    private Map<String, Object> buildPayload(LogEntryDTO dto) {
        return Map.of(
            "alert_type",  "CRITICAL_LOG",
            "service",     dto.getService(),
            "level",       dto.getLevel(),
            "message",     dto.getMessage(),  // NOTE: raw (pre-redaction) for alert fidelity
            "timestamp",   dto.resolvedTimestamp().toString(),
            "trace_id",    dto.getTraceId()  != null ? dto.getTraceId()  : "N/A",
            "span_id",     dto.getSpanId()   != null ? dto.getSpanId()   : "N/A",
            "sent_at",     Instant.now().toString()
        );
    }
}
