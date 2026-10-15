package com.electronik54.consumer.listener;

import com.electronik54.consumer.service.MessageService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MessageListener {

    private final MessageService messageService;

    public MessageListener(MessageService messageService) {
        this.messageService = messageService;
    }

    @KafkaListener(topics = "local.learning.smoke.v1")
    public void onMessage(ConsumerRecord<String, String> record) {
        log.info("Received partition={} offset={} key={} value={} \n"
                , record.partition()
                , record.offset()
                , record.key()
                , record.value()
        );
        messageService.process(record.value());
    }

}
