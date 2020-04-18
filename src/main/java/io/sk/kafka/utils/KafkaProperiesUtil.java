package io.sk.kafka.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class KafkaProperiesUtil {

    private final static String BOOTSTRAP_SERVER_URL="localhost:9092";
    private static Properties producerProperties;
    private static Properties consumerProperties;

    public static final String FIRST_TOPIC = "firsttopic";
    public static final String BOYS_GROUP_ID = "boys";


    public static Properties getProduerProperties(){
        if(producerProperties==null) {
            producerProperties = new Properties();
            producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_URL);
            producerProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProperties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }
        return producerProperties;
    }

    public  static Properties getConsumerProperties(){
        if(consumerProperties==null) {
            consumerProperties = new Properties();
            consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_URL);
            consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.setProperty(GROUP_ID_CONFIG, BOYS_GROUP_ID);
            consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        }
        return consumerProperties;
    }
}
