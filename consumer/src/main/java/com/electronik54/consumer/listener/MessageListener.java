package com.electronik54.consumer.listener;

import com.electronik54.consumer.dto.OrderEvent;
import com.electronik54.consumer.service.MessageService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class MessageListener implements ConsumerSeekAware {

    private final MessageService messageService;

    public MessageListener(MessageService messageService) {
        this.messageService = messageService;
    }

    /*
    IMP-NOTES:
    numPartitions defaults to 1, so Spring creates -retry and -dlt with ONE partition even when the main topic has 3. A failed record is republished to the SAME partition number, so a mismatch breaks the retry. Always pin it: numPartitions = "3".
    @RetryableTopic dynamically generates multiple underlying listener containers (one for your main topic, one for each retry stage, and one for the DLT). So each instance will now run 3 listeners by default
    */
    @RetryableTopic(attempts = "3", numPartitions = "3")
    @KafkaListener(id = "orders-listener", groupId = "learning-consumer", topics = "local.learning.smoke.v1")
    public void onMessage(ConsumerRecord<String, OrderEvent> record) {
        log.info("\nReceived topic={} partition={} offset={} key={} value={} \n"
                , record.topic()
                , record.partition()
                , record.offset()
                , record.key()
                , record.value()
        );
        messageService.process(record.value());
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        // This fires whenever this consumer instance is successfully assigned partitions
        if (assignments.isEmpty()) {
            log.warn(">>> Consumer connected, but NO partitions were assigned (idle instance).");
        } else {
            log.info(">>> Consumer successfully assigned partitions:{} ",assignments.keySet());
        }
    }
}