package io.sk.kafka.tutorial4;

import com.google.common.collect.Lists;
import com.twitter.hbc.core.Client;
import io.sk.kafka.utils.KafkaProperiesUtil;
import io.sk.kafka.utils.ProducerDemoCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.sk.kafka.utils.KafkaProperiesUtil.TWITTER_TWEETS;

public class TwitterStreamConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TwitterStreamConsumer.class);
    public static void main(String[] args) {
        logger.info("welcome to twitter stream consumer");
        //instantiate client
        BlockingQueue<String> msgQueue = new ArrayBlockingQueue<>(10);
        List<String> terms = Lists.newArrayList("coronavirus");
        Client client = new TwitterClient(System.getProperty("consumerKey"),System.getProperty("consumerSecret"),
                System.getProperty("token"), System.getProperty("secret"),terms).getClientInstance(msgQueue);

        //connect the client
        client.connect();



        //create a producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer(KafkaProperiesUtil.getAdvancedProduerProperties());

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("shutdown hook triggered, closing client and prodcuer");
            client.stop();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                logger.error("exception while sleeping", e);
            }
            logger.info("client stopped");
            kafkaProducer.close();
            logger.info("closed producer, exiting");
        }));

        //read messages
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("error while retrieving the tweet");
                client.stop();
            }
            kafkaProducer.send(new ProducerRecord<>(TWITTER_TWEETS,null,msg), new ProducerDemoCallback());
            //logger.info(msg);
        }

        logger.info("Exiting application");
    }
}
