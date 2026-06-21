package com.example.logguard.service;

import com.example.logguard.entity.LogEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class S3ParquetExportServiceTest {

    private S3ParquetExportService service;
    private S3Client s3Client;
    private AthenaClient athenaClient;

    @BeforeEach
    void setUp() {
        service = new S3ParquetExportService();
        s3Client = mock(S3Client.class);
        athenaClient = mock(AthenaClient.class);

        ReflectionTestUtils.setField(service, "bucket", "log-guard-test");
        ReflectionTestUtils.setField(service, "batchSize", 1);
        ReflectionTestUtils.setField(service, "athenaResultsBucket", "s3://athena-results/");
        ReflectionTestUtils.setField(service, "s3Client", s3Client);
        ReflectionTestUtils.setField(service, "athenaClient", athenaClient);
    }

    @Test
    void disabledExportDoesNotBufferOrCallAws() {
        ReflectionTestUtils.setField(service, "s3Enabled", false);

        service.buffer(logEntry());
        service.scheduledFlush();
        service.onShutdown();

        verifyNoInteractions(s3Client, athenaClient);
    }

    @Test
    void successfulExportUploadsParquetAndRepairsAthenaPartitions() {
        ReflectionTestUtils.setField(service, "s3Enabled", true);

        service.buffer(logEntry());

        ArgumentCaptor<PutObjectRequest> request = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client).putObject(request.capture(), any(RequestBody.class));
        verify(athenaClient).startQueryExecution(any(StartQueryExecutionRequest.class));
        assertThat(request.getValue().bucket()).isEqualTo("log-guard-test");
        assertThat(request.getValue().key())
            .startsWith("logs/year=2026/month=06/day=21/service=billing_service/")
            .endsWith(".parquet");
    }

    @Test
    void failedExportIsSwallowedSoPostgresqlProcessingCanContinue() {
        ReflectionTestUtils.setField(service, "s3Enabled", true);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenThrow(S3Exception.builder().message("S3 unavailable").build());

        assertThatCode(() -> service.buffer(logEntry())).doesNotThrowAnyException();
        verifyNoInteractions(athenaClient);
    }

    private static LogEntry logEntry() {
        Instant timestamp = Instant.parse("2026-06-21T08:00:00Z");
        return LogEntry.builder()
            .id(UUID.randomUUID())
            .service("Billing Service")
            .level("INFO")
            .message("Export me")
            .timestamp(timestamp)
            .traceId("trace-1")
            .processedAt(timestamp)
            .build();
    }
}
