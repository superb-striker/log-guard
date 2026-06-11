package com.example.logguard.listener;

import com.example.logguard.dto.LogEntryDTO;
import com.example.logguard.entity.LogEntry;
import com.example.logguard.repository.LogEntryRepository;
import com.example.logguard.service.AlertService;
import com.example.logguard.service.PersonallyIdentifiableInfoRedactionService;
import com.example.logguard.service.S3ParquetExportService;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Instant;

/**
 * LogWorkerListener - the core processing engine.
 *
 * Per-message pipeline:
 *  1. Receive DTO from RabbitMQ (manual ACK mode)
 *  2. PII Redaction via regex patterns + checksum validation
 *  3. Persist redacted entry to PostgreSQL
 *  4. Buffer to S3 data lake (Parquet, async)
 *  5. If level == CRITICAL -> fire async webhook alert
 *  6. ACK the message (success path)
 *     OR NACK without requeue (failure path -> DLX -> DLQ)
 *
 * Manual ACK is critical: it ensures that if the JVM crashes mid-processing,
 * RabbitMQ will redeliver the message to another consumer.
 */
@Component
public class LogWorkerListener {

    private static final Logger log = LoggerFactory.getLogger(LogWorkerListener.class);
    private static final String CRITICAL = "CRITICAL";

    private final PersonallyIdentifiableInfoRedactionService  redactionService;
    private final LogEntryRepository   repository;
    private final AlertService         alertService;
    private final S3ParquetExportService s3ExportService;

    public LogWorkerListener(
            PersonallyIdentifiableInfoRedactionService  redactionService,
            LogEntryRepository   repository,
            AlertService         alertService,
            S3ParquetExportService s3ExportService) {
        this.redactionService = redactionService;
        this.repository       = repository;
        this.alertService     = alertService;
        this.s3ExportService = s3ExportService;
    }

    /**
     * Main listener - binds to guard.logs.queue.
     *
     * @param dto         deserialized log entry DTO
     * @param channel     AMQP channel for manual ACK/NACK
     * @param deliveryTag unique tag for ACK/NACK
     */
    @RabbitListener(
        queues         = "${guard.rabbitmq.queue}",
        containerFactory = "rabbitListenerContainerFactory"
    )
    public void processLog(
            LogEntryDTO dto,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) 
    throws IOException {
    	/**
    	 * LogEntryDTO dto : the incoming log message. Spring automatically converts RabbitMQ JSON into Java object.
    	 * Channel channel : Low-level RabbitMQ connection channel. Used for: ACK = success, NACK = failure
    	 * deliveryTag : Unique RabbitMQ ID for this message. Needed to tell RabbitMQ: remove it / retry it / dead-letter it
    	 */
        log.debug("Worker received: service=[{}] level=[{}]", dto.getService(), dto.getLevel());

        try {
            // Step 1: PII Redaction 
            String redactedMessage = redactionService.redact(dto.getMessage());

            // Step 2: Persist to PostgreSQL
            LogEntry entity = LogEntry.builder()
                .service(dto.getService())
                .level(dto.getLevel())
                .message(redactedMessage)
                .timestamp(dto.resolvedTimestamp())
                .traceId(dto.getTraceId())
                .spanId(dto.getSpanId())
                .metadata(dto.getMetadata())
                .processedAt(Instant.now())
                .build();

            // Step 3: CRITICAL alerting (async, before persist so alert fires even if DB is temporarily slow)
            boolean isCritical = CRITICAL.equalsIgnoreCase(dto.getLevel());
            if (isCritical) {
                // alertService.sendCriticalAlert is @Async - returns immediately
                alertService.sendCriticalAlert(dto);
                entity.setAlertSent(true);
            }

            repository.saveAndFlush(entity);   // This stores final clean log in database. Spring JPA converts: Java object -> SQL INSERT
            s3ExportService.buffer(entity);    // // Dual-write to S3 data lake (async buffer, non-blocking)

            log.info("Processed log id=[{}] service=[{}] level=[{}] critical=[{}]",
                entity.getId(), entity.getService(), entity.getLevel(), isCritical);

            // Step 4: ACK - remove from queue
            channel.basicAck(deliveryTag, false);
        } catch (Exception ex) {
            log.error("Failed to process log - routing to DLQ. Error: {}", ex.getMessage(), ex);

            // NACK without requeue -> triggers DLX routing to guard.logs.dlq
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
