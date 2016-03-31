package com.github.rbrugier.spring.consumer;

import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
public class SoutKafkaListener {

    @KafkaListener(topicPattern = "test")
    public void process(String message) {
        System.out.println("received: " + message);
    }
}
