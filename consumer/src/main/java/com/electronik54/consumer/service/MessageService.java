package com.electronik54.consumer.service;

import com.electronik54.consumer.downstream.PartnerNotifier;
import com.electronik54.consumer.dto.OrderEvent;
import com.electronik54.consumer.exception.SimulatedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class MessageService {

    private final PartnerNotifier partnerNotifier;
    private final CircuitBreakerRegistry circuitBreakerRegistry;

    public void process(OrderEvent event) {
        log.info("Processing orderDate:{} | orderId:{} |  customerId:{} | amount:{}"
                , event.orderDate()
                , event.orderId()
                , event.customerId()
                , event.amount());
        if(event.orderId().contains("poison")){
            throw new SimulatedException("Simulated Kafka message failure for a <poison> message");
        }

        // The breaker wraps ONLY the partner call - never the poison check above.
        CircuitBreaker breaker = circuitBreakerRegistry.circuitBreaker("downstream");
        breaker.executeRunnable(() -> partnerNotifier.notifyPartner(event));
    }

}
