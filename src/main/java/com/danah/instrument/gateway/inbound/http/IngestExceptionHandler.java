package com.danah.instrument.gateway.inbound.http;

import com.danah.instrument.gateway.inbound.IngestException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class IngestExceptionHandler {

    @ExceptionHandler(IngestException.class)
    public ResponseEntity<Map<String, Object>> handleIngest(IngestException ex) {
        HttpStatus status = HttpStatus.resolve(ex.getStatus());
        if (status == null) {
            status = HttpStatus.BAD_REQUEST;
        }
        return ResponseEntity.status(status).body(Map.of(
                "error", status == HttpStatus.UNAUTHORIZED ? "unauthorized" : "bad_request",
                "message", ex.getMessage()
        ));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleUnreadable(HttpMessageNotReadableException ex) {
        return ResponseEntity.badRequest().body(Map.of(
                "error", "bad_request",
                "message", "malformed JSON body"
        ));
    }
}
