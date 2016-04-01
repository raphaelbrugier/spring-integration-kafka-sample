package com.github.rbrugier.raw;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class MainProducer {

    private static final Logger logger = LoggerFactory.getLogger(MainProducer.class);
    public static final String TOPIC_NAME = "test";
    public static final String HOST = "localhost";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", HOST + ":9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        logger.info("starting");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        List<PartitionInfo> testTopicPartitions = producer.partitionsFor(TOPIC_NAME);

        System.out.println("partitions number = " + testTopicPartitions.size());

        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "value-" + i);
            producer.send(record);
//            sleep();
        }

        producer.close();

        logger.info("ended");
    }

    private static void sleep() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
