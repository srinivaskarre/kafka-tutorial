package io.sk.kafka.tutorial1;

import io.sk.kafka.utils.KafkaProperiesUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private final static Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main(String[] args) {
        logger.info("Welcone to the world of Kafka");

        Properties properties = KafkaProperiesUtil.getProduerProperties();
        //connect to to cluster
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);
        //send messages
        int i = 0;
        while(i<1000){
            ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>("firsttopic", "message-"+i++);
            kafkaProducer.send(producerRecord, new ProducerDemoCallback());
        }

        //flush and close
        kafkaProducer.flush();
        kafkaProducer.close();


    }
}
