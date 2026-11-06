package com.electronik54.consumer.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;

public record OrderEvent(String orderId, String customerId, int amount, LocalDateTime orderDate) {
}
