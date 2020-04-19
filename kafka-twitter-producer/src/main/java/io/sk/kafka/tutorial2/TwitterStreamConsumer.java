package io.sk.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.core.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterStreamConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TwitterStreamConsumer.class);
    public static void main(String[] args) {
        logger.info("welcome to twitter stream consumer");
        //instantiate client
        BlockingQueue<String> msgQueue = new ArrayBlockingQueue<>(10);
        List<String> terms = Lists.newArrayList("Coronavirus");
        Client client = new TwitterClient(System.getProperty("consumerKey"),System.getProperty("consumerSecret"),
                System.getProperty("token"), System.getProperty("secret"),terms).getClientInstance(msgQueue);

        //connect the client

        client.connect();

        //read messages
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("error while retrieving the tweet");
                client.stop();
            }
            logger.info(msg);
        }

        logger.info("Exiting application");
    }
}
