package io.sk.kafka.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class KafkaProperiesUtil {

    private final static String BOOTSTRAP_SERVER_URL="localhost:9092";
    private static Properties producerProperties;
    private static Properties consumerProperties;
    private static Properties advancedProducerProperties;

    public static final String FIRST_TOPIC = "firsttopic";
    public static final String TWITTER_TWEETS = "twitter-tweets";
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

    public static Properties getAdvancedProduerProperties(){
        if(advancedProducerProperties==null) {
            advancedProducerProperties = new Properties();
            advancedProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_URL);
            advancedProducerProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            advancedProducerProperties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            /**
             * Safe producer configs
             * idemptence - even producer sends duplicate messages during retriee in n/w error , kafka commits only once
             * acks - all - leader and min.insync.replcias needs to acknowledge
             * no of retries and max concurrent sends at an instance of time
             */
            advancedProducerProperties.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");
            advancedProducerProperties.setProperty(ACKS_CONFIG, "all");
            advancedProducerProperties.setProperty(RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
            advancedProducerProperties.setProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

            /**
             * High throughput configs
             * compression - uses few CPU cycles on producer and consumer but better n/w latency
             * batch size - size of compressed messages
             */
            /*advancedProducerProperties.setProperty(COMPRESSION_TYPE_CONFIG, "snappy");
            advancedProducerProperties.setProperty(BATCH_SIZE_CONFIG, Integer.toString(32*1024));//32 KB
            advancedProducerProperties.setProperty(LINGER_MS_CONFIG, "20");*/
        }
        return advancedProducerProperties;
    }

    public  static Properties getConsumerProperties(){
        if(consumerProperties==null) {
            consumerProperties = new Properties();
            consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_URL);
            consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.setProperty(GROUP_ID_CONFIG, BOYS_GROUP_ID);
            consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
            /**
             * manual offeset and manual pull records size
             */
            consumerProperties.setProperty(MAX_POLL_RECORDS_CONFIG, "100");
            consumerProperties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
        }
        return consumerProperties;
    }
}
