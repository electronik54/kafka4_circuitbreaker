package com.electronik54.producer.dto;

public record PublishResponse(String topic, int partition, long offset) {
}
