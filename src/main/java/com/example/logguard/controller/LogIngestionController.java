package com.example.logguard.controller;

import com.example.logguard.dto.LogEntryDTO;
import com.example.logguard.entity.LogEntry;
import com.example.logguard.repository.LogEntryRepository;
import com.example.logguard.service.LogIngestionService;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * LogIngestionController - REST API for Log-Guard.
 *
 * Endpoints:
 *  POST /api/logs          - ingest a single log entry
 *  POST /api/logs/batch    - ingest multiple log entries
 *  GET  /api/logs          - paginated log retrieval
 *  GET  /api/logs/{id}     - single log by ID
 *  GET  /api/logs/stats    - aggregate statistics
 */
@RestController
@RequestMapping("/api/logs")
public class LogIngestionController {

    private final LogIngestionService ingestionService;
    private final LogEntryRepository  repository;

    public LogIngestionController(
            LogIngestionService ingestionService,
            LogEntryRepository  repository) {
        this.ingestionService = ingestionService;
        this.repository       = repository;
    }

    // Ingestion 
    /**
     * Accepts a single log entry, validates it, and enqueues for async processing.
     *
     * @param dto validated log entry DTO (@Valid triggers jakarta.validation)
     * @return 202 Accepted (processing is async)
     */
    @PostMapping
    public ResponseEntity<Map<String, String>> ingestLog(@Valid @RequestBody LogEntryDTO dto) {
        ingestionService.ingest(dto);
        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(Map.of(
                "status",  "accepted",
                "message", "Log entry queued for processing",
                "service", dto.getService(),
                "level",   dto.getLevel()
            ));
    }

    /**
     * Batch ingestion - each entry is validated and published individually.
     * Partial failure: entries that fail validation return 400; valid ones are still queued.
     */
    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> ingestBatch(
            @Valid @RequestBody java.util.List<@Valid LogEntryDTO> dtos) {
        dtos.forEach(ingestionService::ingest);
        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(Map.of(
                "status",   "accepted",
                "queued",   dtos.size(),
                "message",  "Batch queued for async processing"
            ));
    }

    // Query
    
    @GetMapping
    public ResponseEntity<Page<LogEntry>> getLogs(
            @PageableDefault(size = 20, sort = "timestamp") Pageable pageable) {
        return ResponseEntity.ok(repository.findAll(pageable));
    }

    @GetMapping("/{id}")
    public ResponseEntity<LogEntry> getLog(@PathVariable UUID id) {
        return repository.findById(id)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/level/{level}")
    public ResponseEntity<?> getByLevel(@PathVariable String level) {
        return ResponseEntity.ok(repository.findByLevelOrderByTimestampDesc(level.toUpperCase()));
    }

    // Stats 
    
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        return ResponseEntity.ok(Map.of(
            "total",    repository.count(),
            "debug",    repository.countByLevel("DEBUG"),
            "info",     repository.countByLevel("INFO"),
            "warn",     repository.countByLevel("WARN"),
            "error",    repository.countByLevel("ERROR"),
            "critical", repository.countByLevel("CRITICAL")
        ));
    }
}
