package com.electronik54.consumer.exception;

import com.electronik54.consumer.downstream.DownstreamUnavailableException;
import com.electronik54.consumer.dto.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(SimulatedException.class)
    public ResponseEntity<ErrorResponse> handleSimulatedException(SimulatedException ex, HttpServletRequest request) {
        log.error("{{SIMULATED EXCEPTION}}:{}",ex.getMessage());
        return build(HttpStatus.SERVICE_UNAVAILABLE,
                "Downstream service error",
                request);
    }

    @ExceptionHandler(DownstreamUnavailableException.class)
    public ResponseEntity<ErrorResponse> handleDownstreamUnavailableException(DownstreamUnavailableException ex, HttpServletRequest request) {
        log.error("Downstream service error");
        return build(HttpStatus.SERVICE_UNAVAILABLE,
                "Downstream service error",
                request);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadable(HttpMessageNotReadableException ex, HttpServletRequest request) {
        log.error("Malformed request body on {} ({})",
                request.getRequestURI()
                , ex.getMostSpecificCause().getClass().getSimpleName());
        return build(HttpStatus.BAD_REQUEST,
                "Malformed request body: the JSON is invalid or a field has the wrong type",
                request);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(Exception ex, HttpServletRequest request) {
        log.error("Unhandled error: {}", ex.getMessage(), ex);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "Unexpected error: " + ex.getClass().getSimpleName(), request);
    }

    private ResponseEntity<ErrorResponse> build(HttpStatus status, String message, HttpServletRequest request) {
        ErrorResponse body = new ErrorResponse(
                Instant.now(), status.value(), status.getReasonPhrase(), message, request.getRequestURI());
        return ResponseEntity.status(status).body(body);
    }
}
