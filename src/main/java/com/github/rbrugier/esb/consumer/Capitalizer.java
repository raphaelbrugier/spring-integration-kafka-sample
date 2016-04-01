package com.github.rbrugier.esb.consumer;

import com.github.rbrugier.esb.MessageWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;


@Service
public class Capitalizer {

    @Autowired
    private KafkaTemplate<Integer, MessageWrapper> responseTemplate;


    @KafkaListener(topicPattern = "test")
    public void process(@Payload MessageWrapper message) {
        System.out.println("received: " + message.getValue());
        MessageWrapper wrapper = new MessageWrapper(message.getCommandId(), message.getGroupId(), message.getValue().toUpperCase());
        responseTemplate.convertAndSend("response", wrapper);
    }
}
