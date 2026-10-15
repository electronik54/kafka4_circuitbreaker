package com.electronik54.producer.controller;

import com.electronik54.producer.dto.PublishRequest;
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
    public ResponseEntity<PublishResponse> publish(@RequestBody PublishRequest req){
        PublishResponse res = messagePublisher.publish(TOPIC, req.key(), req.value());
        return ResponseEntity.status(HttpStatus.CREATED).body(res);
    }
}
