package com.example.logguard.repository;

import com.example.logguard.model.LogEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * This repository manages LogEntry records.
 * By extending JpaRepository, you automatically get built-in methods like: save(), findById(), findAll(), deleteById() etc.
 * So this interface adds custom queries on top of default CRUD.
 */

@Repository
public interface LogEntryRepository extends JpaRepository<LogEntry, UUID> {

    // Spring reads the method name and generates SQL
	List<LogEntry> findByLevelOrderByTimestampDesc(String level);

    List<LogEntry> findByServiceOrderByTimestampDesc(String service);

    @Query("""
        SELECT l FROM LogEntry l
        WHERE l.timestamp BETWEEN :from AND :to
        ORDER BY l.timestamp DESC
    """)
    List<LogEntry> findBetween(
        @Param("from") Instant from,
        @Param("to")   Instant to
    );

    @Query("SELECT COUNT(l) FROM LogEntry l WHERE l.level = :level")
    long countByLevel(@Param("level") String level);

    List<LogEntry> findByAlertSentFalseAndLevel(String level);
}
