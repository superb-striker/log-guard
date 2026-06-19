package com.example.logguard.service;

import com.example.logguard.dto.LogEntryDTO;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.retry.annotation.Retryable;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AlertServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicInteger status = new AtomicInteger(202);
    private final List<String> requestBodies = new ArrayList<>();

    private HttpServer server;
    private AlertService service;

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/alerts", exchange -> {
            requestBodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] response = "response".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status.get(), response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();

        service = new AlertService(WebClient.builder().build());
        ReflectionTestUtils.setField(service, "webhookUrl",
            "http://127.0.0.1:" + server.getAddress().getPort() + "/alerts");
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void deduplicatesWithinCooldownAndReportsSuppressedCountInNextAlert() throws Exception {
        LogEntryDTO alert = criticalLog("billing-service");

        service.sendCriticalAlert(alert);
        service.sendCriticalAlert(alert);
        assertThat(requestBodies).hasSize(1);

        Map<String, Instant> lastAlertTime = (Map<String, Instant>) ReflectionTestUtils
            .getField(service, "lastAlertTime");
        assertThat(lastAlertTime).isNotNull();
        lastAlertTime.put("billing-service", Instant.now().minusSeconds(61));

        service.sendCriticalAlert(alert);

        assertThat(requestBodies).hasSize(2);
        JsonNode followUp = objectMapper.readTree(requestBodies.get(1));
        assertThat(followUp.path("suppressed_count").asInt()).isEqualTo(1);

        Map<String, AtomicInteger> suppressed = (ConcurrentHashMap<String, AtomicInteger>)
            ReflectionTestUtils.getField(service, "suppressedCounts");
        assertThat(suppressed.get("billing-service")).hasValue(0);
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401})
    void clientErrorsAreConfiguredAsNonRetryable(int responseStatus) throws Exception {
        status.set(responseStatus);

        assertThatThrownBy(() -> service.sendCriticalAlert(criticalLog("auth-service")))
            .isInstanceOf(WebClientResponseException.class);
        assertThat(requestBodies).hasSize(1);

        Retryable retryable = AlertService.class
            .getMethod("sendCriticalAlert", LogEntryDTO.class)
            .getAnnotation(Retryable.class);
        assertThat(Arrays.asList(retryable.noRetryFor())).contains(
            WebClientResponseException.BadRequest.class,
            WebClientResponseException.Unauthorized.class);
    }

    private static LogEntryDTO criticalLog(String serviceName) {
        return LogEntryDTO.builder()
            .service(serviceName)
            .level("CRITICAL")
            .message("Dependency unavailable")
            .build();
    }
}
