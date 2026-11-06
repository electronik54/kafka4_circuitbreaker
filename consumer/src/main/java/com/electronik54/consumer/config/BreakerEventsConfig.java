package com.electronik54.consumer.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class BreakerEventsConfig {
    public BreakerEventsConfig(CircuitBreakerRegistry registry) {
        CircuitBreaker breaker = registry.circuitBreaker("downstream");
        breaker.getEventPublisher().onStateTransition(event -> log.warn(
                "BREAKER {} state:{} -> {}",
                event.getCircuitBreakerName(),
                event.getStateTransition().getFromState(),
                event.getStateTransition().getToState()));
    }
}
