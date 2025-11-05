package com.example.logguard.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration
public class WebClientConfig {

    private static final Logger log = LoggerFactory.getLogger(WebClientConfig.class);

    @Value("${guard.webhook.timeout-seconds:10}")
    private int timeoutSeconds;

    @Bean
    WebClient webhookWebClient() {
        HttpClient httpClient = HttpClient.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutSeconds * 1000)  // Max time allowed to establish connection
            .responseTimeout(Duration.ofSeconds(timeoutSeconds))                  // Once request sent: how long to wait for response. 
            .doOnConnected(conn -> conn
                .addHandlerLast(new ReadTimeoutHandler(timeoutSeconds,  TimeUnit.SECONDS))    // Stalled response timeout
                .addHandlerLast(new WriteTimeoutHandler(timeoutSeconds, TimeUnit.SECONDS))    // Stalled request timeout
            );

        return WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(httpClient))    // Use the custom timeout-configured client
            .defaultHeader("User-Agent",    "Log-Guard/1.0")
            .defaultHeader("Content-Type",  "application/json")
            .filter(loggingFilter())       // Adds request interceptor. Runs before request sent.
            .build();
    }

    // Log outbound requests and response status for observability.
    private ExchangeFilterFunction loggingFilter() {
        return ExchangeFilterFunction.ofRequestProcessor(req -> {
            log.debug("-> Webhook request: {} {}", req.method(), req.url());
            return Mono.just(req);
        });
    }
}
