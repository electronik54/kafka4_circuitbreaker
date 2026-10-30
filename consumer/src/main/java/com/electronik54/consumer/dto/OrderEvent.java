package com.electronik54.consumer.dto;

public record OrderEvent(String orderId, String customerId, int amount) {
}
