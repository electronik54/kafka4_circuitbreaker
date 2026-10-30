package com.electronik54.producer.dto;

public record OrderEvent(String orderId, String customerId, int amount) {
}
