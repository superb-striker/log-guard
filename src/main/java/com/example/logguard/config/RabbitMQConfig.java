package com.example.logguard.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * RabbitMQ topology for Log-Guard.
 *
 * Exchange topology (now sharded - see mainExchange() / shardQueuesAndBindings()):
 *
 *  [Producer]
 *      │
 *      ▼  routing-key: log.ingest.shard-{0..N-1}   (computed by LogIngestionService)
 *  [guard.exchange]  (direct)
 *      │
 *      ├──> [guard.logs.queue.shard-0]
 *      ├──> [guard.logs.queue.shard-1]
 *      ├──> [guard.logs.queue.shard-2]
 *      └──> [guard.logs.queue.shard-3]
 *              │  x-dead-letter-exchange: guard.dlx  (shared DLX/DLQ across all shards)
 *              │  x-dead-letter-routing-key: log.dead
 *              │
 *  On failure / NACK / TTL expiry:
 *      │
 *      ▼  routing-key: log.dead
 *  [guard.dlx]  (direct)
 *      │
 *      ▼
 *  [guard.logs.dlq]   <- inspect failed messages here (dead-letter queue, shared across shards)
 *
 * NOTE: guard.rabbitmq.queue-prefix is the base name used here to declare ALL
 * shard queues. guard.rabbitmq.queue (used by LogWorkerListener) is a
 * different, per-instance property that resolves to exactly one of the
 * queues declared below via the SHARD_ID env var - see application.yml.
 */
@Configuration
public class RabbitMQConfig {
	
    // Load values from application.yml

    @Value("${guard.rabbitmq.exchange}")
    private String exchange;
 
    @Value("${guard.rabbitmq.queue-prefix}")
    private String queuePrefix;
 
    @Value("${guard.rabbitmq.routing-key}")
    private String routingKeyPrefix;
 
    @Value("${guard.rabbitmq.shard-count:4}")
    private int shardCount;
 
    @Value("${guard.rabbitmq.dlx}")
    private String dlx;
 
    @Value("${guard.rabbitmq.dlq}")
    private String dlq;
 
    @Value("${guard.rabbitmq.dlq-routing-key}")
    private String dlqRoutingKey;

    // Exchanges
    /** Direct exchange routes messages based on exact routing key
	durable=true ensures exchange survives RabbitMQ restart. **/
    
    @Bean
    public DirectExchange mainExchange() {
    	
        return ExchangeBuilder
            .directExchange(exchange)     
            .durable(true)
            .build();
    }

    @Bean
    public DirectExchange deadLetterExchange() {
        return ExchangeBuilder
            .directExchange(dlx)
            .durable(true)
            .build();
    }

    // Shard queues + bindings
    /**
     * Declares one durable queue per shard (guard.logs.queue.shard-0 ...
     * shard-{N-1}), each bound to mainExchange() with its own routing key
     * (log.ingest.shard-0, etc.), each with the same DLX args the original
     * single mainQueue() had. All shards share one DLX/DLQ - simpler to
     * inspect failed messages in one place, and failure handling doesn't
     * depend on which shard produced the failure.
     *
     * Declarables lets us register a variable number of beans (driven by
     * shard-count) from a single @Bean method - Spring AMQP declares every
     * Declarable in the returned collection against the broker on startup.
     */
    @Bean
    public Declarables shardQueuesAndBindings(DirectExchange mainExchange) {
        List<Declarable> declarables = new ArrayList<>();
 
        for (int shardIndex = 0; shardIndex < shardCount; shardIndex++) {
            String shardQueueName = queuePrefix + ".shard-" + shardIndex;
            String shardRoutingKey = routingKeyPrefix + ".shard-" + shardIndex;
 
            Queue shardQueue = QueueBuilder
                .durable(shardQueueName)
                .withArgument("x-dead-letter-exchange",    dlx)
                .withArgument("x-dead-letter-routing-key", dlqRoutingKey)
                .withArgument("x-message-ttl",              18_000_000)   // 30 min TTL - unchanged from original
                .withArgument("x-max-length",               500_000)     // unchanged from original
                .build();
 
            Binding shardBinding = BindingBuilder
                .bind(shardQueue)
                .to(mainExchange)
                .with(shardRoutingKey);
 
            declarables.add(shardQueue);
            declarables.add(shardBinding);
        }
 
        return new Declarables(declarables);
    }

    // Dead Letter Queue - durable, no DLX of its own to avoid loops.
     
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder
            .durable(dlq)
            .build();
    }

    @Bean
    public Binding dlqBinding(Queue deadLetterQueue, DirectExchange deadLetterExchange) {
        return BindingBuilder
            .bind(deadLetterQueue)
            .to(deadLetterExchange)
            .with(dlqRoutingKey);
    }

    // Message Converter

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new JacksonJsonMessageConverter();    // converts POJO object to JSON
    }

    // App uses this to send messages
    
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter());
        template.setObservationEnabled(true);
        // If exchange cannot route message: RabbitMQ notifies sender. Without this, message may silently disappear.
        template.setMandatory(true);
        return template;
    }

    /**
     * Listener container factory with manual ACK and JSON converter.
     * Manual ACK is essential so we can NACK on processing failure,
     * triggering the DLX routing.
     */
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jsonMessageConverter());
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setPrefetchCount(10);            // Consumer fetches max 10 unacknowledged messages at once
        factory.setConcurrentConsumers(5);       // ADD - 5 threads always running
        factory.setMaxConcurrentConsumers(20);   // ADD - burst up to 20 under load
        factory.setObservationEnabled(true);
        return factory;
    }
}
