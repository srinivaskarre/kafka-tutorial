package io.sk.kafka.tutorial1;

import io.sk.kafka.utils.KafkaProperiesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static io.sk.kafka.utils.KafkaProperiesUtil.FIRST_TOPIC;

public class ConsumerDemoWithThread implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    private final CountDownLatch countDownLatch;

    private KafkaConsumer<String,String> kafkaConsumer;

    public ConsumerDemoWithThread(CountDownLatch countDownLatch){
        this.countDownLatch = countDownLatch;
    }
    @Override
    public void run() {
        try{
            kafkaConsumer = new KafkaConsumer(KafkaProperiesUtil.getConsumerProperties());
            kafkaConsumer.subscribe(Collections.singleton(KafkaProperiesUtil.FIRST_TOPIC));
            while (true){
                ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                records.forEach(rec ->logger.info(
                        "key="+ rec.key()
                                +", Partition="+rec.partition()
                                +", offset="+rec.offset()
                                +", key ="+rec.key()
                ));
            }
        }catch (WakeupException e){
            logger.error("got wakeup exception",e);
            kafkaConsumer.close();
            countDownLatch.countDown();
        }
    }

    public  void shutdown(){
        kafkaConsumer.wakeup();
    }
}
