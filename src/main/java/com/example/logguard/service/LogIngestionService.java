package com.example.logguard.service;

import com.example.logguard.dto.LogEntryDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * LogIngestionService - validates and publishes inbound log DTOs to RabbitMQ.
 *
 * The REST layer hands a validated DTO here; this service stamps a timestamp
 * (if absent) and publishes to the main exchange.  Actual processing - redaction,
 * persistence, alerting - happens asynchronously in the worker listener.
 */
@Service
public class LogIngestionService {

    private static final Logger log = LoggerFactory.getLogger(LogIngestionService.class);

    private final RabbitTemplate rabbitTemplate;

    @Value("${guard.rabbitmq.exchange}")
    private String exchange;

    @Value("${guard.rabbitmq.routing-key}")
    private String routingKeyPrefix;

    @Value("${guard.rabbitmq.shard-count:4}")
    private int shardCount;

    public LogIngestionService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    
    // Publishes a log entry to the AMQP exchange after validating through dto.
   	
    public void ingest(LogEntryDTO dto) {
        // Ensure timestamp is always set
        if (dto.getTimestamp() == null) {
            dto.setTimestamp(Instant.now());
        }

        log.info("Ingesting log: service=[{}] level=[{}] traceId=[{}]",
            dto.getService(), dto.getLevel(), dto.getTraceId());

        int shard = Math.floorMod(dto.getService().hashCode(), shardCount);
        String routingKey = routingKeyPrefix + ".shard-" + shard;
        rabbitTemplate.convertAndSend(exchange, routingKey, dto);

        log.debug("Published to exchange=[{}] routingKey=[{}]", exchange, routingKey);
    }
}
