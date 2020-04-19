package io.sk.kafka.consumer;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import io.sk.kafka.utils.KafkaProperiesUtil;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

import static io.sk.kafka.utils.KafkaProperiesUtil.TWITTER_TWEETS;

public class ElasticSearchConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    private static RestHighLevelClient setupRestClient(){
        RestClientBuilder clientBuilder = RestClient.builder(
                new HttpHost("localhost",9200,"http"));
        RestHighLevelClient client = new RestHighLevelClient(clientBuilder);
        return client;
    }

    public static KafkaConsumer<String, String> setupConsumerAndSubscribe(){
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(KafkaProperiesUtil.getConsumerProperties());
        kafkaConsumer.subscribe(Arrays.asList(TWITTER_TWEETS));
        return kafkaConsumer;
    }

    public static void main(String[] args) throws IOException {
        logger.info("start>>>");
        RestHighLevelClient client = setupRestClient();
        KafkaConsumer<String,String> kafkaConsumer = setupConsumerAndSubscribe();
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(5));
                if(consumerRecords.count()>0)
                    logger.info("total records pulled="+consumerRecords.count());
                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> rec : consumerRecords) {
                    if(rec==null) continue;
                    String id = extractIdFromTweets(rec);
                    if(id==null) continue;
                    IndexRequest indexRequest = new IndexRequest("twitter").source(rec.value(), XContentType.JSON).id(id);
                    bulkRequest.add(indexRequest);
                    /**
                     * below commented are like indexing document by document
                     */
                    //IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
                    //logger.info("id indexed ="+id+", version"+response.getVersion());
                    //Thread.sleep(5000L);
                }
                if(consumerRecords.count()>0){
                    BulkResponse bulkResponse = client.bulk(bulkRequest,RequestOptions.DEFAULT);
                    for(BulkItemResponse response: bulkResponse.getItems()) {
                        logger.info("document indexed with id="+response.getId());
                    }
                    /**
                     * committing manually offset
                     */
                    kafkaConsumer.commitSync();
                    /**
                     * TODO wait for sometime while producer some data
                     */
                    Thread.sleep(1000L);
                }

            }
        }catch (Exception e){
            logger.error("exception while consuming/indexing data",e);
        }finally {
            client.close();
            kafkaConsumer.close();
        }

    }
    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweets(ConsumerRecord<String, String> rec) {
        String key = null;
        try {
            key = jsonParser.parse(rec.value()).getAsJsonObject().get("id_str").getAsString();
        }catch (NullPointerException e){
            logger.warn("swallow it--->");
        }
        return key;

    }
}
