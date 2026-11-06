package com.electronik54.consumer.resilience;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Slf4j
public class DownstreamBreakerGuard {

    private static final String BREAKER_NAME = "downstream";
    private static final String LISTENER_ID_PREFIX = "orders-listener";

    private final CircuitBreaker circuitBreaker;
    private final KafkaListenerEndpointRegistry registry;
    private final AtomicBoolean paused = new AtomicBoolean(false);

    public DownstreamBreakerGuard(CircuitBreakerRegistry circuitBreakerRegistry,
                                  KafkaListenerEndpointRegistry registry) {
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(BREAKER_NAME);
        this.registry = registry;
    }

    @PostConstruct
    void registerEventListeners() {
        log.info("Breaker guard active | breaker='{}' | listenerIdPrefix='{}'",
                circuitBreaker.getName(), LISTENER_ID_PREFIX);
        circuitBreaker.getEventPublisher().onStateTransition(event -> {
            CircuitBreaker.State to = event.getStateTransition().getToState();
            apply(to);
        });
    }

    @Scheduled(fixedDelayString = "${app.downstream.guard.check-interval-ms:5000}")
    void reconcile() {
        apply(circuitBreaker.getState());
    }

    private void apply(CircuitBreaker.State state) {
        if (state == CircuitBreaker.State.OPEN) {
            pauseContainers();
        } else {
            resumeContainers();
        }
    }

    private void pauseContainers() {
        if (paused.compareAndSet(false, true)) {
            List<MessageListenerContainer> containers = managedContainers();
            containers.forEach(MessageListenerContainer::pause);
            log.warn("Breaker OPEN -> PAUSED {} container(s): {}", containers.size(), managedContainerIds());
        }
    }

    private void resumeContainers() {
        if (paused.compareAndSet(true, false)) {
            List<MessageListenerContainer> containers = managedContainers();
            containers.forEach(MessageListenerContainer::resume);
            log.warn("Breaker {} -> RESUMED {} container(s): {}",
                    circuitBreaker.getState(), containers.size(), managedContainerIds());
        }
    }

    private List<MessageListenerContainer> managedContainers() {
        return registry.getListenerContainers().stream()
                .filter(c -> c.getListenerId() != null && c.getListenerId().startsWith(LISTENER_ID_PREFIX))
                .toList();
    }

    public List<String> managedContainerIds() {
        return managedContainers().stream().map(MessageListenerContainer::getListenerId).toList();
    }

    public boolean isPaused() {
        return paused.get();
    }

    @EventListener(ApplicationReadyEvent.class)
    void logManagedContainers() {
        log.info("Guard manages {} container(s): {}", managedContainerIds().size(), managedContainerIds());
    }
}
