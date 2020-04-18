package io.sk.kafka.tutorial1;

import io.sk.kafka.utils.KafkaProperiesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static io.sk.kafka.utils.KafkaProperiesUtil.FIRST_TOPIC;

public class ConsumerDempAssignAndSeek {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDempAssignAndSeek.class);
    public static void main(String[] args) {
        Properties properties = KafkaProperiesUtil.getConsumerProperties();
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);

        //assign to a topic partition than subscribing
        TopicPartition topicPartition = new TopicPartition(FIRST_TOPIC,0);
        long offToReadFrom = 15L; // reads from offset 15
        kafkaConsumer.assign(Arrays.asList(topicPartition));

        //seek
        kafkaConsumer.seek(topicPartition,offToReadFrom);

        boolean consumeMessages = true;
        int messagesToRead = 5;
        int readCounter = 0;

        while (consumeMessages){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord rec : records) {
                logger.info("key=" + rec.key()
                        + ", Partition=" + rec.partition()
                        + ", offset=" + rec.offset()
                        + ", key =" + rec.key());
                if(++readCounter>messagesToRead){
                    consumeMessages = false;
                    break;
                }
            }
        }
    }
}
