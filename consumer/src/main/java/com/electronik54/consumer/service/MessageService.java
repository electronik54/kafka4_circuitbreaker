package com.electronik54.consumer.service;

import com.electronik54.consumer.dto.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class MessageService {

    public void process(OrderEvent event) {
        log.info("Processing orderId:{} |  customerId:{} | amount:{} \n", event.orderId(), event.customerId(), event.amount());
        if(event.orderId().contains("poison")){
            throw new RuntimeException("Simulated Kafka message failure for a <poison> message");
        }
    }

}
