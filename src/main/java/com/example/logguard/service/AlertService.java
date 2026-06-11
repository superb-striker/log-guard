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

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AlertService - sends asynchronous webhook notifications for CRITICAL logs.
 *
 * Resilience strategy:
 *  • @Async         - non-blocking; doesn't stall the worker thread
 *  • @Retryable     - exponential backoff: 1s -> 2s -> 4s (max 3 attempts)
 *  • @Recover       - graceful fallback logged when all retries are exhausted
 *  • Deduplication  - per-service 60s window: only the first alert fires the
 *                     webhook; subsequent occurrences are counted and included
 *                     in the NEXT alert payload so no information is lost.
 *
 * Deduplication vs suppression:
 *   Suppression drops subsequent alerts entirely - dangerous in production
 *   because 50 failures look identical to 1. Deduplication counts them and
 *   surfaces the total in the next outgoing alert, giving on-call engineers
 *   full context without flooding the webhook endpoint.
 *
 * The separation of @Async and @Retryable requires that this service is called
 * from another Spring bean (proxy boundary), not from within the same class.
 */
@Service
public class AlertService {

    private static final Logger log = LoggerFactory.getLogger(AlertService.class);

    // Deduplication window - one webhook call per service per minute maximum
    private static final long COOLDOWN_SECONDS = 60;

    // Tracks when the last alert was dispatched per service
    private final Map<String, Instant> lastAlertTime = new ConcurrentHashMap<>();

    // Counts alerts suppressed during the cooldown window per service.
    // Reset to 0 each time a new alert passes the cooldown and fires.
    private final Map<String, AtomicInteger> suppressedCounts = new ConcurrentHashMap<>();

    private final WebClient webClient;

    @Value("${guard.webhook.url}")
    private String webhookUrl;

    // App may have multiple WebClients. @Qualifier ensures correct timeout config.
    public AlertService(@Qualifier("webhookWebClient") WebClient webClient) {
        this.webClient = webClient;
    }

    /**
     * Dispatches a CRITICAL alert to the configured webhook endpoint.
     *
     * Deduplication logic:
     *   - First alert for a service passes immediately and fires the webhook.
     *   - Subsequent alerts within COOLDOWN_SECONDS increment a suppressed
     *     counter and are logged but do not hit the webhook.
     *   - When the cooldown expires, the next alert fires with a
     *     "suppressed_count" field showing how many were held back.
     *     The counter then resets to 0 for the new window.
     *
     * Retry policy:
     *  - maxAttempts = 3, initialDelay = 1000ms, multiplier = 2.0 (1s->2s->4s)
     *  - jitter enabled to prevent thundering herd on retry storms
     *  - no retry on 400/401 (client errors, retrying won't help)
     *
     * @param dto the original (pre-redaction) DTO for alert payload construction
     */
    @Async
    @Retryable(
        retryFor   = { Exception.class },
        noRetryFor = { WebClientResponseException.BadRequest.class,
                       WebClientResponseException.Unauthorized.class },
        maxAttempts = 3,
        backoff = @Backoff(
            delay      = 1000,
            multiplier = 2.0,
            maxDelay   = 30_000,
            random     = true
        )
    )
    public void sendCriticalAlert(LogEntryDTO dto) {
        Instant last = lastAlertTime.get(dto.getService());

        if (last != null && Duration.between(last, Instant.now()).toSeconds() < COOLDOWN_SECONDS) {
            // Within cooldown window - count but don't fire
            int count = suppressedCounts
                .computeIfAbsent(dto.getService(), k -> new AtomicInteger(0))
                .incrementAndGet();
            log.warn("Alert deduplicated for service=[{}] - {} occurrence(s) suppressed this window",
                dto.getService(), count);
            return;
        }

        // Cooldown expired or first alert - drain suppressed count for this service
        int previouslySuppressed = suppressedCounts
            .getOrDefault(dto.getService(), new AtomicInteger(0))
            .getAndSet(0);

        lastAlertTime.put(dto.getService(), Instant.now());

        log.warn("CRITICAL alert firing for service=[{}] - {} occurrence(s) were suppressed in previous window",
            dto.getService(), previouslySuppressed);

        webClient.post()
            .uri(webhookUrl)
            .bodyValue(buildPayload(dto, previouslySuppressed))
            .retrieve()
            .bodyToMono(String.class)
            .doOnSuccess(resp -> log.info("Webhook alert delivered for service=[{}]", dto.getService()))
            .doOnError(err  -> log.error("Webhook alert delivery failed: {}", err.getMessage()))
            .block(); // block is acceptable inside @Async thread
    }

    /**
     * Recovery method - invoked after all @Retryable attempts are exhausted.
     * TODO: publish failed alerts to a fallback queue for incident auditing.
     */
    @Recover
    public void recoverAlert(Exception ex, LogEntryDTO dto) {
        log.error(
            "ALERT DELIVERY FAILED after all retries - service=[{}] level=[{}] error=[{}]",
            dto.getService(), dto.getLevel(), ex.getMessage()
        );
    }

    /**
     * Builds the webhook payload.
     *
     * @param dto                 the triggering log entry
     * @param previouslySuppressed number of alerts held back in the previous
     *                            cooldown window - surfaced so on-call engineers
     *                            know the true occurrence count, not just 1
     */
    private Map<String, Object> buildPayload(LogEntryDTO dto, int previouslySuppressed) { 
        Map<String, Object> payload = new HashMap<>();
        payload.put("alert_type",          "CRITICAL_LOG");
        payload.put("service",             dto.getService());
        payload.put("level",               dto.getLevel());
        payload.put("message",             dto.getMessage()); // NOTE: raw (pre-redaction) for alert fidelity
        payload.put("timestamp",           dto.resolvedTimestamp().toString());
        payload.put("trace_id",            dto.getTraceId() != null ? dto.getTraceId() : "N/A");
        payload.put("span_id",             dto.getSpanId()  != null ? dto.getSpanId()  : "N/A");
        payload.put("sent_at",             Instant.now().toString());
        payload.put("suppressed_count",    previouslySuppressed);  // 0 on first alert, N on subsequent windows
        return payload;
    }
}