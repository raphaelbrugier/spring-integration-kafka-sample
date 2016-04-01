package com.github.rbrugier.esb.producer;

import com.github.rbrugier.esb.MessageWrapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
public class ResponseHandler {

    public static abstract class Observer {

        private final String commandId;

        protected Observer(String commandId) {
            this.commandId = commandId;
        }

        abstract void accept(String value);
    }

    private List<Observer> observers = Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(topicPattern = "response")
    public void handleResponse(MessageWrapper messageWrapper) {
        for (Observer observer : observers) {
            if (observer.commandId.equals(messageWrapper.getCommandId())) {
                try {
                    observer.accept(messageWrapper.getValue());
                } finally {
                    detachObserver(observer);
                }
            }
        }
    }

    public void attach(Observer observer) {
        observers.add(observer);
    }

    private void detachObserver(Observer observer) {
        observers.remove(observer);
    }
}
