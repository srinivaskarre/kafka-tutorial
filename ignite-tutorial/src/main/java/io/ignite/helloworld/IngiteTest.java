package io.ignite.helloworld;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.apache.ignite.stream.kafka.KafkaStreamer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;

public class IngiteTest {

    public static void main(String[] args) throws InterruptedException {

        IgniteConfiguration cfg = new IgniteConfiguration();

        // The node will be started as a client node.
        cfg.setClientMode(true);

        // Classes of custom Java logic will be transferred over the wire from this app.
        cfg.setPeerClassLoadingEnabled(true);

        // Setting up an IP Finder to ensure the client can locate the servers.
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));

        // Starting the node
        Ignite ignite = Ignition.start(cfg);


        KafkaStreamer<String, String> kafkaStreamer = new KafkaStreamer<>();

        IgniteDataStreamer<String, String> stmr = ignite.dataStreamer("mycache");

// allow overwriting cache data
        stmr.allowOverwrite(true);

        kafkaStreamer.setIgnite(ignite);
        kafkaStreamer.setStreamer(stmr);

// set the topic
        kafkaStreamer.setTopic(Arrays.asList("twitter-tweets"));

// set the number of threads to process Kafka streams
        kafkaStreamer.setThreads(4);

        Properties properties = KafkaProperiesUtil.getConsumerProperties();

// set Kafka consumer configurations
        kafkaStreamer.setConsumerConfig(properties);

// set extractor
        kafkaStreamer.setSingleTupleExtractor(new StreamSingleTupleExtractor<ConsumerRecord, String, String>() {
            @Override
            public Map.Entry<String, String> extract(ConsumerRecord consumerRecord) {
                String key = extractIdFromTweets(consumerRecord);
                String val = consumerRecord.value().toString();
                System.out.println("================================================"+key+"====> value: "+consumerRecord.value());
                return new AbstractMap.SimpleEntry<String, String>(key, val);
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("shutdown hook triggered, closing client and prodcuer");
            stmr.flush();
            kafkaStreamer.stop();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
            stmr.close();

            //kafkaStreamer.close();
            ignite.close();

        }));

        kafkaStreamer.start();
        System.out.println("Streamer started========>");


        Thread.sleep(10000000L);




// stop on shutdown
        kafkaStreamer.stop();




    }

    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweets(ConsumerRecord<String, String> rec) {
        String key = null;
        try {
            key = jsonParser.parse(rec.value()).getAsJsonObject().get("id_str").getAsString();
        }catch (NullPointerException e){
            //logger.warn("swallow it--->");
        }
        return key;

    }
}
