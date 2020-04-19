package io.sk.kafka.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoCallback implements Callback {
    private Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if(e==null){
            logger.info("record sent successfully, to topic=" + recordMetadata.topic()
                    + ", partition=" + recordMetadata.partition()
                    + ", offeset=" + recordMetadata.offset() + ", timestamp=" + recordMetadata.timestamp());
        }else{
            logger.error("exception while sending the data",e);
        }
    }
}
