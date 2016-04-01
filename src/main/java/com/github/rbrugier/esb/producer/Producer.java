package com.github.rbrugier.esb.producer;


import com.github.rbrugier.esb.MessageWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.SimpleKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.github.rbrugier.esb.producer.ResponseHandler.*;

@SpringBootApplication
@EnableKafka
public class Producer {

    private static final String GROUP_ID = "someGroup";
    @Value("${kafka.broker.address}")
    private String brokerAddress;

    @Value("${kafka.topic.name}")
    private String topic;

    @Autowired
    private KafkaTemplate<Integer, MessageWrapper> template;

    @Autowired
    ResponseHandler responseHandler;

    public static void main(String[] args) {
        ConfigurableApplicationContext context
                = new SpringApplicationBuilder(Producer.class)
                .web(false)
                .run(args);

        Producer app = context.getBean(Producer.class);

        app.send();
    }

    public void send() {
        for (int i = 0; i <10 ; i++) {
            String value = "value-" + i;
            String commandId = genCommandId();
            MessageWrapper message = new MessageWrapper(commandId, GROUP_ID, value);
            responseHandler.attach(new ResponseHandler.Observer(commandId) {
                @Override
                void accept(String value) {
                    System.out.println(value);
                }
            });
            try {
                template.syncConvertAndSend(topic, message);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private String genCommandId() {
        return UUID.randomUUID().toString();
    }

    @Bean
    public KafkaTemplate<Integer, MessageWrapper> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    public ProducerFactory<Integer, MessageWrapper> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    private Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.github.rbrugier.esb.serial.SerializableSerializer");
        return props;
    }


    @Bean
    public SimpleKafkaListenerContainerFactory kafkaListenerContainerFactory() {
        SimpleKafkaListenerContainerFactory factory = new SimpleKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(4);
        return factory;
    }

    public ConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_" + UUID.randomUUID().toString());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.github.rbrugier.esb.serial.SerializableDeserializer");
        return props;
    }
}
