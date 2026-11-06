package com.electronik54.producer.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;

public record OrderEvent(String orderId, String customerId, int amount, LocalDateTime orderDate) {
    public OrderEvent {
        if (orderDate == null) {
            orderDate = LocalDateTime.now();
        }
    }
    public OrderEvent(String orderId, String customerId, int amount) {
        this(orderId, customerId, amount, LocalDateTime.now());
    }
}
