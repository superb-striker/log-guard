package com.example.logguard.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class PersonallyIdentifiableInfoRedactionServiceTest {

    private PersonallyIdentifiableInfoRedactionService service;

    @BeforeEach
    void setUp() {
        service = new PersonallyIdentifiableInfoRedactionService();
        ReflectionTestUtils.setField(service, "redactionEnabled", true);
    }

    @ParameterizedTest(name = "redacts {0}")
    @MethodSource("piiExamples")
    void redactsEverySupportedPiiCategory(String category, String value, String replacement) {
        assertThat(service.redact("PII " + value)).isEqualTo("PII " + replacement);
    }

    static Stream<Arguments> piiExamples() {
        return Stream.of(
            Arguments.of("JWT", "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.signature", "[TOKEN REDACTED]"),
            Arguments.of("email", "jane.doe+alerts@example.com", "[EMAIL REDACTED]"),
            Arguments.of("IPv4", "192.168.10.25", "[IP REDACTED]"),
            Arguments.of("Indian phone", "+91 98765 43210", "[PHONE REDACTED]"),
            Arguments.of("credit card", "4111111111111111", "[CC REDACTED]"),
            Arguments.of("Aadhaar", "2345 6789 0124", "[AADHAAR REDACTED]")
        );
    }

    @Test
    void preservesCardAndAadhaarCandidatesWithInvalidChecksums() {
        String message = "order=4000000000000000 reference=2345 6789 0123";

        assertThat(service.redact(message)).isEqualTo(message);
    }
}
