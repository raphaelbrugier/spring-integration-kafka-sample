package com.github.rbrugier.esb.consumer;

import com.github.rbrugier.esb.MessageWrapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;


@Service
public class Capitalizer {

    @KafkaListener(topicPattern = "test")
    public void process(@Payload MessageWrapper message) {
        System.out.println("received: " + message.getValue().toUpperCase());
    }
}
