package io.sk.kafka;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;

public class TwitterKafkaStream {
    private static final Logger logger = LoggerFactory.getLogger(TwitterKafkaStream.class);
    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(APPLICATION_ID_CONFIG, "demo-kafka-streams");

        //build topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String,String> inputTopic = streamsBuilder.stream("twitter-tweets");
        KStream<String,String> filteredStream = inputTopic.filter((k,v)-> extractFollowersCount(v)>10000);
        filteredStream.to("important-tweets");

        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        //start application
        kafkaStreams.start();

    }

    private static JsonParser jsonParser = new JsonParser();

    private static Integer extractFollowersCount(String v) {
        try{
            return jsonParser.parseString(v).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
        }catch (NullPointerException e){
            logger.warn("bad data->"+v);
            return 0;
        }
    }
}
