package com.github.rbrugier.raw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MainConsumer {

    public static final String TOPIC_NAME = "test";
    public static final String GROUP_NAME = "mygroup";
    public static final String HOST = "localhost";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", HOST + ":9092");
        props.put("group.id", GROUP_NAME);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        boolean running = true;
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                int partitionId = record.partition();
                System.out.println("partition id = " + partitionId + " value = " + record.value());
            }
        }

        consumer.close();
    }
}
