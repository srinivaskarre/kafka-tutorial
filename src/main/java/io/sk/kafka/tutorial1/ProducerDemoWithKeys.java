package io.sk.kafka.tutorial1;

import io.sk.kafka.utils.KafkaProperiesUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static io.sk.kafka.utils.KafkaProperiesUtil.FIRST_TOPIC;

public class ProducerDemoWithKeys {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("Welcome to ProducerDemo with keys");

        Properties properties = KafkaProperiesUtil.getProduerProperties();

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer(properties);

        int i=0;
        String msg = "message-";
        String key = "key-";
        while(++i<100) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord(FIRST_TOPIC, key+i, msg+i);

            kafkaProducer.send(producerRecord, new ProducerDemoCallback()).get();//don;t do get, this get is future get
        }
    }
}
