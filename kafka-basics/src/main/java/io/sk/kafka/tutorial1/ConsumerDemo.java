package io.sk.kafka.tutorial1;

import io.sk.kafka.utils.KafkaProperiesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

import static io.sk.kafka.utils.KafkaProperiesUtil.FIRST_TOPIC;

public class ConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        logger.info("Welcome to consumer");

        CountDownLatch countDownLatch = new CountDownLatch(1);
        ConsumerDemoWithThread consumerDemoWithThread = new ConsumerDemoWithThread(countDownLatch);
        Thread thread = new Thread(consumerDemoWithThread);
        thread.start();
        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("application shutdown hook");
            consumerDemoWithThread.shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                logger.error("",e);
            }
            logger.info("Application has exited");
        }));

        try{
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("exception while waiting for consumers", e);
        }finally {
            logger.info("Application runtime has completed successfully, closing the application");
        }

    }
}
