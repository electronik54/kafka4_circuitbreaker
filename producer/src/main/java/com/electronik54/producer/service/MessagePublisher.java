package com.electronik54.producer.service;

import com.electronik54.producer.dto.OrderEvent;
import com.electronik54.producer.dto.PublishResponse;
import com.electronik54.producer.exception.PublishException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class MessagePublisher {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public MessagePublisher(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public PublishResponse publish(String topic, String key, OrderEvent event) {
        try {
            RecordMetadata meta = kafkaTemplate.send(topic, key, event).join().getRecordMetadata();
            log.debug("Published to {} partition={} offset={}", meta.topic(), meta.partition(), meta.offset());
            return new PublishResponse(meta.topic(), meta.partition(), meta.offset());
        } catch (Exception ex) {
            throw new PublishException("Failed to publish to topic " + topic, ex);
        }
    }
}
