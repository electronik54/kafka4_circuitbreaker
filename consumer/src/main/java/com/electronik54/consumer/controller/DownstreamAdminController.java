package com.electronik54.consumer.controller;

import com.electronik54.consumer.downstream.PartnerNotifier;
import com.electronik54.consumer.dto.PartnerStatusRequest;
import com.electronik54.consumer.resilience.DownstreamBreakerGuard;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("api/downstream")
@RequiredArgsConstructor
public class DownstreamAdminController {

    private static String BREAKER_NAME="downstream";

    private final PartnerNotifier partnerNotifier;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final DownstreamBreakerGuard downstreamBreakerGuard;
    private final Environment environment;


    @PostMapping("/setStatus")
    public ResponseEntity<Map<String, Object>> setPartnerService(@RequestBody PartnerStatusRequest value) {
        return ResponseEntity.ok(Map.of("partnerStatus", partnerNotifier.partnerStatus(value.down())));
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        CircuitBreaker breaker = circuitBreakerRegistry.circuitBreaker(BREAKER_NAME);
        CircuitBreaker.Metrics m = breaker.getMetrics();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("instancePort", environment.getProperty("server.port", "unknown"));
        body.put("partnerDown", partnerNotifier.partnerStatus());
        body.put("breakerState", breaker.getState().name());
        body.put("containersPaused", downstreamBreakerGuard.isPaused());
        body.put("containerIds", downstreamBreakerGuard.managedContainerIds());
        body.put("bufferedCalls", m.getNumberOfBufferedCalls());
        body.put("failedCalls", m.getNumberOfFailedCalls());
        body.put("notPermittedCalls", m.getNumberOfNotPermittedCalls());
        body.put("failureRatePercent", m.getFailureRate());
        return ResponseEntity.ok(body);
    }

}
