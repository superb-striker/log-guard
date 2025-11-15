package com.example.logguard.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Centralised error handling
 */
@RestControllerAdvice    // Watches all REST controllers. If any exception happens, routes it here.
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * Handles jakarta.validation constraint violations from @Valid on request bodies.
     * Returns HTTP 400 with a structured map of field -> error message.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidationErrors(MethodArgumentNotValidException ex) {
    	/**
    	 * Spring stores all invalid fields. 
    	 * Example:
    	 * Field	Error
    	 * service	must not be blank
    	 * level	invalid level
    	 */
        Map<String, String> fieldErrors = ex.getBindingResult()   
            .getFieldErrors()
            .stream()
            .collect(Collectors.toMap(
                FieldError::getField,
                fe -> fe.getDefaultMessage() != null ? fe.getDefaultMessage() : "Invalid value",
                (existing, replacement) -> existing  // keep first on duplicate field
            ));

        log.warn("Validation failed: {}", fieldErrors);

        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        detail.setTitle("Validation Failed");
        detail.setDetail("One or more fields failed validation");
        detail.setProperty("timestamp",    Instant.now().toString());
        detail.setProperty("fieldErrors",  fieldErrors);
        return detail;
    }

    /** Catch-all for unexpected runtime exceptions */
    @ExceptionHandler(Exception.class)
    public ProblemDetail handleGenericException(Exception ex) {
        log.error("Unhandled exception: {}", ex.getMessage(), ex);

        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.INTERNAL_SERVER_ERROR);
        detail.setTitle("Internal Server Error");
        detail.setDetail("An unexpected error occurred. Please try again.");
        detail.setProperty("timestamp", Instant.now().toString());
        return detail;
    }
}
