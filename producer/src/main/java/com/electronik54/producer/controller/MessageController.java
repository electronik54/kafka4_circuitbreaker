package com.electronik54.producer.controller;

import com.electronik54.producer.dto.OrderEvent;
import com.electronik54.producer.dto.PublishResponse;
import com.electronik54.producer.service.MessagePublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/messages")
public class MessageController {

    private static final String TOPIC="local.learning.smoke.v1";

    private final MessagePublisher messagePublisher;
    public MessageController(MessagePublisher msgPub) {
        this.messagePublisher = msgPub;
    }

    @PostMapping(value = "publish")
    public ResponseEntity<PublishResponse> publish(@RequestBody OrderEvent event){
        String key = event.orderId(); // NOTE: the key decides which partition does the message goes to, to maintain the sequence of messages as required. eg: order id is the key so that the sequence of order in cart > order payment done > order placed > order shipped are all processed in the same partition and then processed sequentially by the same consumer
        PublishResponse res = messagePublisher.publish(TOPIC, key, event);
        return ResponseEntity.status(HttpStatus.CREATED).body(res);
    }
}
