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

/**
 * RabbitMQ topology for Log-Guard.
 *
 * Exchange topology:
 *
 *  [Producer]
 *      │
 *      ▼  routing-key: log.ingest
 *  [guard.exchange]  (direct)
 *      │
 *      ▼
 *  [guard.logs.queue]
 *      │  x-dead-letter-exchange: guard.dlx
 *      │  x-dead-letter-routing-key: log.dead
 *      │
 *  On failure / NACK / TTL expiry:
 *      │
 *      ▼  routing-key: log.dead
 *  [guard.dlx]  (direct)
 *      │
 *      ▼
 *  [guard.logs.dlq]   <- inspect failed messages here (dead-letter queue)
 */
@Configuration
public class RabbitMQConfig {
	
    // Load values from application.yml

    @Value("${guard.rabbitmq.exchange}")
    private String exchange;

    @Value("${guard.rabbitmq.queue}")
    private String queue;

    @Value("${guard.rabbitmq.routing-key}")
    private String routingKey;

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

    // Queues

    /**
     * Main processing queue with DLX headers.
     * Messages NACK'd (without requeue) are forwarded to guard.dlx.
     */
    @Bean
    public Queue mainQueue() {
        return QueueBuilder
            .durable(queue)
            .withArgument("x-dead-letter-exchange",     dlx)
            .withArgument("x-dead-letter-routing-key",  dlqRoutingKey)
            .withArgument("x-message-ttl",              300_000)   // 5 min TTL
            .withArgument("x-max-length",               100_000)   // back-pressure cap : max 100k messages
            .build();
    }

    // Dead Letter Queue - durable, no DLX of its own to avoid loops.
     
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder
            .durable(dlq)
            .build();
    }

    // Bindings

    @Bean
    public Binding mainBinding(Queue mainQueue, DirectExchange mainExchange) {
        return BindingBuilder
            .bind(mainQueue)
            .to(mainExchange)
            .with(routingKey);
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
        factory.setPrefetchCount(10);  // Consumer fetches max 10 unacknowledged messages at once
        factory.setConcurrentConsumers(5);       // ADD - 5 threads always running
        factory.setMaxConcurrentConsumers(20);   // ADD - burst up to 20 under load
        factory.setObservationEnabled(true);
        return factory;
    }
}
