package com.example.logguard;

import com.example.logguard.entity.LogEntry;
import com.example.logguard.repository.LogEntryRepository;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LogGuardIntegrationTests {

    private static final String EXCHANGE = "guard.test.exchange";
    private static final String QUEUE = "guard.test.queue.shard-0";
    private static final String ROUTING_KEY = "log.test.ingest.shard-0";
    private static final String DLQ = "guard.test.dlq";

    private static final AtomicInteger WEBHOOK_REQUESTS = new AtomicInteger();
    private static final AtomicInteger WEBHOOK_STATUS = new AtomicInteger(202);
    private static final AtomicReference<String> LAST_WEBHOOK_BODY = new AtomicReference<>();
    private static final HttpServer WEBHOOK_SERVER = startWebhookServer();

    @Container
    static final PostgreSQLContainer POSTGRES =
        new PostgreSQLContainer("postgres:16-alpine")
            .withDatabaseName("log_guard_test")
            .withUsername("guard")
            .withPassword("guard_secret");

    @Container
    static final RabbitMQContainer RABBITMQ =
        new RabbitMQContainer("rabbitmq:3.13-management-alpine")
            .withAdminUser("guard")
            .withAdminPassword("guard_secret");

    @DynamicPropertySource
    static void registerContainerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");

        registry.add("spring.rabbitmq.host", RABBITMQ::getHost);
        registry.add("spring.rabbitmq.port", RABBITMQ::getAmqpPort);
        registry.add("spring.rabbitmq.username", RABBITMQ::getAdminUsername);
        registry.add("spring.rabbitmq.password", RABBITMQ::getAdminPassword);
        registry.add("guard.rabbitmq.exchange", () -> EXCHANGE);
        registry.add("guard.rabbitmq.queue-prefix", () -> "guard.test.queue");
        registry.add("guard.rabbitmq.queue", () -> QUEUE);
        registry.add("guard.rabbitmq.routing-key", () -> "log.test.ingest");
        registry.add("guard.rabbitmq.shard-count", () -> 1);
        registry.add("guard.rabbitmq.dlx", () -> "guard.test.dlx");
        registry.add("guard.rabbitmq.dlq", () -> DLQ);
        registry.add("guard.rabbitmq.dlq-routing-key", () -> "log.test.dead");

        registry.add("guard.webhook.url", () -> webhookUrl());
        registry.add("guard.webhook.timeout-seconds", () -> 1);
        registry.add("guard.s3.enabled", () -> false);
        registry.add("guard.s3.bucket", () -> "unused");
        registry.add("guard.s3.region", () -> "ap-south-1");
        registry.add("aws.access-key", () -> "unused");
        registry.add("aws.secret-key", () -> "unused");
    }

    @LocalServerPort
    int port;

    @Autowired
    LogEntryRepository repository;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    AmqpAdmin amqpAdmin;

    @BeforeEach
    void resetState() {
        repository.deleteAll();
        amqpAdmin.purgeQueue(QUEUE, true);
        amqpAdmin.purgeQueue(DLQ, true);
        WEBHOOK_REQUESTS.set(0);
        WEBHOOK_STATUS.set(202);
        LAST_WEBHOOK_BODY.set(null);

        RestAssured.port = port;
        RestAssured.basePath = "/api/logs";
    }

    @AfterAll
    static void stopWebhookServer() {
        WEBHOOK_SERVER.stop(0);
        RestAssured.reset();
    }

    @Test
    void validLogReturns202Accepted() {
        given()
            .contentType(ContentType.JSON)
            .body(validLog("acceptance-service", "INFO", "Application started"))
        .when()
            .post()
        .then()
            .statusCode(202)
            .body("status", equalTo("accepted"))
            .body("service", equalTo("acceptance-service"))
            .body("level", equalTo("INFO"));
    }

    @Test
    void invalidSeverityReturns400BadRequest() {
        given()
            .contentType(ContentType.JSON)
            .body(validLog("validation-service", "FATAL", "Unsupported severity"))
        .when()
            .post()
        .then()
            .statusCode(400)
            .body("title", equalTo("Validation Failed"))
            .body("fieldErrors.level", equalTo(
                "Log level must be one of: DEBUG, INFO, WARN, ERROR, CRITICAL."));
    }

    @Test
    void missingRequiredFieldsReturnStructuredValidationErrors() {
        given()
            .contentType(ContentType.JSON)
            .body("{}")
        .when()
            .post()
        .then()
            .statusCode(400)
            .body("title", equalTo("Validation Failed"))
            .body("detail", equalTo("One or more fields failed validation"))
            .body("fieldErrors", hasKey("service"))
            .body("fieldErrors", hasKey("level"))
            .body("fieldErrors", hasKey("message"))
            .body("timestamp", org.hamcrest.Matchers.notNullValue());
    }

    @Test
    void batchIngestionAcceptsAndPersistsValidEntries() {
        given()
            .contentType(ContentType.JSON)
            .body("""
                [
                  {"service":"batch-service","level":"INFO","message":"first"},
                  {"service":"batch-service","level":"WARN","message":"second"}
                ]
                """)
        .when()
            .post("/batch")
        .then()
            .statusCode(202)
            .body("status", equalTo("accepted"))
            .body("queued", equalTo(2));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
            assertThat(repository.findByServiceOrderByTimestampDesc("batch-service")).hasSize(2));
    }

    @Test
    void filteringAndStatisticsEndpointsReturnCorrectData() {
        repository.saveAllAndFlush(List.of(
            persistedLog("query-service", "INFO", "one", Instant.parse("2026-01-01T00:00:00Z")),
            persistedLog("query-service", "ERROR", "two", Instant.parse("2026-01-02T00:00:00Z")),
            persistedLog("other-service", "ERROR", "three", Instant.parse("2026-01-03T00:00:00Z"))
        ));

        given()
        .when()
            .get("/level/error")
        .then()
            .statusCode(200)
            .body("level", org.hamcrest.Matchers.everyItem(equalTo("ERROR")))
            .body("", hasSize(2));

        given()
        .when()
            .get("/stats")
        .then()
            .statusCode(200)
            .body("total", equalTo(3))
            .body("info", equalTo(1))
            .body("error", equalTo(2))
            .body("warn", equalTo(0))
            .body("critical", equalTo(0));
    }

    @Test
    void publishedMessagesAreEventuallyPersisted() {
        given()
            .contentType(ContentType.JSON)
            .body(validLog("persistence-service", "INFO", "Ready for persistence"))
        .when()
            .post()
        .then()
            .statusCode(202);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<LogEntry> logs = repository.findByServiceOrderByTimestampDesc("persistence-service");
            assertThat(logs).hasSize(1);
            assertThat(logs.getFirst().getMessage()).isEqualTo("Ready for persistence");
        });
    }

    @Test
    void piiIsRedactedBeforePostgresqlPersistence() {
        given()
            .contentType(ContentType.JSON)
            .body(validLog(
                "pii-service",
                "INFO",
                "Login for jane.doe@example.com from 192.168.10.25"))
        .when()
            .post()
        .then()
            .statusCode(202);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<LogEntry> logs = repository.findByServiceOrderByTimestampDesc("pii-service");
            assertThat(logs).hasSize(1);
            assertThat(logs.getFirst().getMessage())
                .isEqualTo("Login for [EMAIL REDACTED] from [IP REDACTED]")
                .doesNotContain("jane.doe@example.com", "192.168.10.25");
        });
    }

    @Test
    void invalidRabbitMqMessagesEnterTheDlq() {
        Message invalidMessage = MessageBuilder
            .withBody("{}".getBytes(StandardCharsets.UTF_8))
            .setContentType(MessageProperties.CONTENT_TYPE_JSON)
            .build();

        rabbitTemplate.send(EXCHANGE, ROUTING_KEY, invalidMessage);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
            assertThat(rabbitTemplate.receive(DLQ)).isNotNull());
    }

    @Test
    void criticalLogsInvokeTheWebhook() {
        given()
            .contentType(ContentType.JSON)
            .body(validLog("critical-webhook-service", "CRITICAL", "Database unavailable"))
        .when()
            .post()
        .then()
            .statusCode(202);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(WEBHOOK_REQUESTS).hasValue(1);
            assertThat(LAST_WEBHOOK_BODY.get())
                .contains("\"alert_type\":\"CRITICAL_LOG\"")
                .contains("\"service\":\"critical-webhook-service\"");
        });
    }

    @Test
    void unavailableWebhookRetriesThreeTimesAndRecoversWithoutLosingTheLog() {
        WEBHOOK_STATUS.set(503);

        given()
            .contentType(ContentType.JSON)
            .body(validLog("retry-webhook-service", "CRITICAL", "Dependency unavailable"))
        .when()
            .post()
        .then()
            .statusCode(202);

        await().atMost(Duration.ofSeconds(12)).until(() -> WEBHOOK_REQUESTS.get() >= 3);
        await().during(Duration.ofMillis(750)).atMost(Duration.ofSeconds(3))
            .until(() -> WEBHOOK_REQUESTS.get() == 3);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
            assertThat(repository.findByServiceOrderByTimestampDesc("retry-webhook-service"))
                .hasSize(1));
    }

    private static String validLog(String service, String level, String message) {
        return """
            {
              "service": "%s",
              "level": "%s",
              "message": "%s"
            }
            """.formatted(service, level, message);
    }

    private static LogEntry persistedLog(String service, String level, String message, Instant timestamp) {
        return LogEntry.builder()
            .service(service)
            .level(level)
            .message(message)
            .timestamp(timestamp)
            .processedAt(timestamp)
            .build();
    }

    private static HttpServer startWebhookServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/alerts", LogGuardIntegrationTests::handleWebhook);
            server.start();
            return server;
        } catch (IOException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private static void handleWebhook(HttpExchange exchange) throws IOException {
        WEBHOOK_REQUESTS.incrementAndGet();
        LAST_WEBHOOK_BODY.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));

        byte[] response = "ok".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(WEBHOOK_STATUS.get(), response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private static String webhookUrl() {
        return "http://127.0.0.1:" + WEBHOOK_SERVER.getAddress().getPort() + "/alerts";
    }
}
