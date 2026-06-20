package com.example.logguard.controller;

import com.example.logguard.entity.LogEntry;
import com.example.logguard.exception.GlobalExceptionHandler;
import com.example.logguard.repository.LogEntryRepository;
import com.example.logguard.service.LogIngestionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class LogIngestionControllerTest {

    private LogIngestionService ingestionService;
    private LogEntryRepository repository;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        ingestionService = mock(LogIngestionService.class);
        repository = mock(LogEntryRepository.class);

        LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();

        mockMvc = MockMvcBuilders
            .standaloneSetup(new LogIngestionController(ingestionService, repository))
            .setControllerAdvice(new GlobalExceptionHandler())
            .setValidator(validator)
            .build();
    }

    @Test
    void rejectsEmptyBatchWithoutPublishingMessages() throws Exception {
        mockMvc.perform(post("/api/logs/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content("[]"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.status").value("rejected"))
            .andExpect(jsonPath("$.queued").value(0));

        verifyNoInteractions(ingestionService);
    }

    @Test
    void rejectsEntirePartiallyInvalidBatchWithoutPublishingMessages() throws Exception {
        mockMvc.perform(post("/api/logs/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                    [
                      {"service":"valid-service","level":"INFO","message":"valid"},
                      {"service":"invalid-service","message":"missing level"}
                    ]
                    """))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.title").value("Validation Failed"));

        verifyNoInteractions(ingestionService);
    }

    @Test
    void paginatesLogsInDescendingTimestampOrder() throws Exception {
        LogEntry newest = logAt("newest", Instant.parse("2026-06-20T12:00:00Z"));
        LogEntry older = logAt("older", Instant.parse("2026-06-20T11:00:00Z"));
        when(repository.findAll(any(Pageable.class)))
            .thenAnswer(invocation -> {
                Pageable pageable = invocation.getArgument(0);
                assertThat(pageable.getSort().getOrderFor("timestamp").getDirection())
                    .isEqualTo(Sort.Direction.DESC);
                return new PageImpl<>(List.of(newest, older), pageable, 2);
            });

        mockMvc.perform(get("/api/logs?page=0&size=2"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.content[0].message").value("newest"))
            .andExpect(jsonPath("$.content[1].message").value("older"));
    }

    @Test
    void returnsNotFoundForUnknownLogId() throws Exception {
        UUID unknownId = UUID.randomUUID();
        when(repository.findById(unknownId)).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/logs/{id}", unknownId))
            .andExpect(status().isNotFound());
    }

    private static LogEntry logAt(String message, Instant timestamp) {
        return LogEntry.builder()
            .id(UUID.randomUUID())
            .service("query-service")
            .level("INFO")
            .message(message)
            .timestamp(timestamp)
            .processedAt(timestamp)
            .build();
    }
}
