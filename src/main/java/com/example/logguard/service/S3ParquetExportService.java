package com.example.logguard.service;

import com.example.logguard.entity.LogEntry;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * S3ParquetExportService - dual-write layer for the data lake.
 *
 * After each LogEntry is persisted to PostgreSQL (OLTP), this service buffers
 * it in memory and flushes to S3 as SNAPPY-compressed Parquet under a
 * Hive-style partition key when either the batch size or flush interval is hit:
 *
 *   logs/year=YYYY/month=MM/day=DD/service=<svc>/batch-<uuid>.parquet
 *
 * After each S3 upload, MSCK REPAIR TABLE is fired against Athena so new
 * partitions are immediately queryable without manual intervention.
 *
 * Key design decisions:
 *   - CopyOnWriteArrayList: allows concurrent AMQP consumer threads to call
 *     buffer() without locking; drain is synchronized to prevent double-flush.
 *   - Temp file: Parquet requires a seekable output, so we write locally then
 *     stream to S3 and delete.
 *   - Graceful degradation: S3/Athena failures are logged and swallowed;
 *     PostgreSQL already holds all records so pipeline continuity is preserved.
 *   - @PreDestroy flush: drains the in-flight buffer on clean JVM shutdown.
 */
@Service
public class S3ParquetExportService {

    private static final Logger log = LoggerFactory.getLogger(S3ParquetExportService.class);

    // Avro schema for Parquet serialization.
    // metadata excluded (JSONB - complex Avro type, add later if needed).
    // spanId is optional - not all entries carry a span.
    private static final Schema SCHEMA = SchemaBuilder.record("LogEntry")
        .namespace("com.example.logguard")
        .fields()
            .requiredString("id")
            .requiredString("service")
            .requiredString("level")
            .requiredString("message")
            .requiredLong("timestamp")      // epoch millis
            .requiredString("traceId")
            .optionalString("spanId")       // nullable
            .requiredLong("processedAt")    // epoch millis
        .endRecord();

    // Config

    @Value("${guard.s3.enabled:true}")
    private boolean s3Enabled;

    @Value("${guard.s3.bucket}")
    private String bucket;

    @Value("${guard.s3.region}")
    private String region;

    @Value("${guard.s3.batch-size:100}")
    private int batchSize;

    @Value("${aws.access-key}")
    private String accessKey;

    @Value("${aws.secret-key}")
    private String secretKey;

    @Value("${guard.s3.athena-results-bucket:s3://log-guard-athena-query-results/}")
    private String athenaResultsBucket;

    // State

    private S3Client s3Client;
    private AthenaClient athenaClient;

    // CopyOnWriteArrayList: safe for concurrent writes from multiple AMQP threads.
    private final List<LogEntry> buffer = new CopyOnWriteArrayList<>();

    
    // Lifecycle
    

    @PostConstruct
    public void init() {
        if (!s3Enabled) {
            log.info("S3 export disabled via guard.s3.enabled=false");
            return;
        }

        StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKey, secretKey)
        );

        s3Client = S3Client.builder()
            .region(Region.of(region))
            .credentialsProvider(credentials)
            .build();

        athenaClient = AthenaClient.builder()
            .region(Region.of(region))
            .credentialsProvider(credentials)
            .build();

        log.info("S3ParquetExportService initialized - bucket=[{}] region=[{}] batchSize=[{}]",
            bucket, region, batchSize);
    }

    
    // Public API
    

    /**
     * Buffers a redacted LogEntry for export. Non-blocking - returns immediately.
     * Triggers an immediate flush if batchSize is reached.
     */
    public void buffer(LogEntry entry) {
        if (!s3Enabled) return;

        buffer.add(entry);
        log.debug("Buffered log id=[{}] - buffer size=[{}]", entry.getId(), buffer.size());

        if (buffer.size() >= batchSize) {
            log.info("Batch size [{}] reached - triggering immediate S3 flush", batchSize);
            flushToS3();
        }
    }

    
    // Scheduled + shutdown flushes
    

    /**
     * Time-based flush - ensures data isn't stranded in memory during low-volume periods.
     * Runs every 60 seconds (configurable via guard.s3.flush-interval-ms).
     */
    @Scheduled(fixedDelayString = "${guard.s3.flush-interval-ms:60000}")
    public void scheduledFlush() {
        if (!s3Enabled || buffer.isEmpty()) return;
        log.info("Scheduled flush triggered - buffer size=[{}]", buffer.size());
        flushToS3();
    }

    /**
     * Drains the buffer on clean JVM exit (docker stop / SIGTERM).
     * Prevents losing the tail that hasn't hit batchSize or the next tick.
     */
    @PreDestroy
    public void onShutdown() {
        if (!s3Enabled || buffer.isEmpty()) return;
        log.info("Shutdown detected - flushing remaining [{}] entries to S3", buffer.size());
        flushToS3();
    }

    
    // Core flush logic
    

    /**
     * Drains the buffer atomically, serializes to Parquet, uploads to S3,
     * then triggers Athena partition repair. Synchronized to prevent concurrent
     * flushes (scheduled tick racing with batch threshold trigger).
     */
    private synchronized void flushToS3() {
        if (buffer.isEmpty()) return;

        // Snapshot and clear - new entries during flush go into a fresh buffer.
        List<LogEntry> batch = new ArrayList<>(buffer);
        buffer.clear();

        log.info("Flushing [{}] entries to S3", batch.size());

        Path tempFile = null;
        try {
            tempFile = writeTempParquet(batch);
            String s3Key = buildS3Key(batch.get(0));
            uploadToS3(tempFile, s3Key);
            log.info("S3 upload complete - key=[{}] records=[{}]", s3Key, batch.size());
            repairAthenaPartitions();
        } catch (Exception ex) {
            // Graceful degradation - PostgreSQL holds all records, S3 lag is acceptable.
            log.error("S3 flush failed - [{}] records may be missing from data lake. Error: {}",
                batch.size(), ex.getMessage(), ex);
        } finally {
            deleteTempFile(tempFile);
        }
    }

    
    // Parquet serialization
    

    /**
     * Writes a batch to a local temp Parquet file (SNAPPY-compressed).
     * Temp file approach required because Parquet needs a seekable output stream.
     */
    private Path writeTempParquet(List<LogEntry> batch) throws IOException {
        // createTempFile generates a unique name AND creates the file on disk.
        // LocalOutputFile uses CREATE_NEW which fails if the file already exists.
        // So we delete immediately after to free the path, then let Parquet create it.
        java.io.File temp = java.io.File.createTempFile("log-guard-", ".parquet");
        temp.delete();        // free the path so LocalOutputFile can CREATE_NEW
        temp.deleteOnExit();  // backup cleanup on JVM exit if deleteTempFile fails

        Path tempFile = temp.toPath();

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new LocalOutputFile(tempFile))
                .withSchema(SCHEMA)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .build()) {

            for (LogEntry entry : batch) {
                writer.write(toAvroRecord(entry));
            }
        }

        return tempFile;
    }

    /**
     * Maps a LogEntry to an Avro GenericRecord.
     * Timestamps stored as epoch millis - query in Athena via from_unixtime(ts / 1000).
     */
    private GenericRecord toAvroRecord(LogEntry entry) {
        GenericRecord record = new GenericData.Record(SCHEMA);

        record.put("id",          entry.getId() != null ? entry.getId().toString() : "");
        record.put("service",     entry.getService());
        record.put("level",       entry.getLevel());
        record.put("message",     entry.getMessage());
        record.put("timestamp",   entry.getTimestamp()    != null ? entry.getTimestamp().toEpochMilli()   : 0L);
        record.put("traceId",     entry.getTraceId()      != null ? entry.getTraceId()                    : "");
        record.put("spanId",      entry.getSpanId());  // nullable - Avro optional handles null
        record.put("processedAt", entry.getProcessedAt() != null ? entry.getProcessedAt().toEpochMilli() : 0L);

        return record;
    }

    
    // S3 helpers
    

    /**
     * Builds the Hive-partitioned S3 key for a batch.
     * Format: logs/year=YYYY/month=MM/day=DD/service=<svc>/batch-<uuid>.parquet
     * Partitioning on date + service allows Athena to prune on both dimensions.
     * UUID suffix guarantees uniqueness across concurrent flushes.
     */
    private String buildS3Key(LogEntry firstEntry) {
        Instant ts = firstEntry.getProcessedAt() != null
            ? firstEntry.getProcessedAt()
            : Instant.now();

        ZonedDateTime zdt = ts.atZone(ZoneOffset.UTC);

        return String.format("logs/year=%04d/month=%02d/day=%02d/service=%s/batch-%s.parquet",
            zdt.getYear(),
            zdt.getMonthValue(),
            zdt.getDayOfMonth(),
            sanitizeService(firstEntry.getService()),
            UUID.randomUUID()
        );
    }

    /** Sanitizes service name for safe use in an S3 key. */
    private String sanitizeService(String service) {
        if (service == null || service.isBlank()) return "unknown";
        return service.replaceAll("[^a-zA-Z0-9\\-_]", "_").toLowerCase();
    }

    /** Uploads a local file to S3. Streams from disk - no full in-memory load. */
    private void uploadToS3(Path tempFile, String s3Key) {
        PutObjectRequest request = PutObjectRequest.builder()
            .bucket(bucket)
            .key(s3Key)
            .contentType("application/octet-stream")
            .build();

        s3Client.putObject(request, RequestBody.fromFile(tempFile));
    }

    /** Deletes the temp Parquet file. Always runs via finally block. */
    private void deleteTempFile(Path tempFile) {
        if (tempFile == null) return;
        try {
            Files.deleteIfExists(tempFile);
        } catch (IOException ex) {
            log.warn("Failed to delete temp Parquet file [{}]: {}", tempFile, ex.getMessage());
        }
    }

    
    // Athena partition repair
    

    /**
     * Fires MSCK REPAIR TABLE after each successful S3 upload so new partitions
     * are immediately visible in Athena without manual intervention.
     * startQueryExecution is async on the Athena side - no impact on flush latency.
     */
    private void repairAthenaPartitions() {
        try {
            StartQueryExecutionRequest request = StartQueryExecutionRequest.builder()
                .queryString("MSCK REPAIR TABLE logs")
                .queryExecutionContext(QueryExecutionContext.builder()
                    .database("default")
                    .build())
                .resultConfiguration(ResultConfiguration.builder()
                    .outputLocation(athenaResultsBucket)
                    .build())
                .build();

            athenaClient.startQueryExecution(request);
            log.info("Athena partition repair triggered");
        } catch (Exception ex) {
            log.warn("Athena partition repair failed - run MSCK REPAIR manually: {}", ex.getMessage());
        }
    }
}