package com.electronik54.consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class MessageService {

    public void process(String payload) {
        log.info("Processing payload={} \n", payload);
    }

}
