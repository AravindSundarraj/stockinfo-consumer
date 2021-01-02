package com.group.kaka.stockinfoconsumer.consumer;

import com.group.kaka.stockinfoconsumer.domain.Stock;
import com.group.kaka.stockinfoconsumer.listener.StockListener;
import com.group.kaka.stockinfoconsumer.model.KafkaOffset;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.transaction.Transactional;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.group.kaka.stockinfoconsumer.dao.KafkaOffsetEntity;


@Component
@Slf4j
public class StockConsumer {

    @Autowired
    KafkaOffsetEntity kafkaOffsetEntity;

    private static final String BOOTSTRAP_SERVER = "localhost:9095,localhost:9096,localhost:9097";
    // map to update the offset for each partition
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets =
            new HashMap<>();

    private KafkaConsumer<String, Stock> getConfig() {

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "stock-group");
        props.setProperty("enable.auto.commit", "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");

        log.info("Setting Kafka Consumer Properties for Stock : {} ", props);

        KafkaConsumer<String, Stock> kafkaConsumer = new KafkaConsumer(props);
        return kafkaConsumer;

    }

    /*
    here goal should be to commit process the record and commit the offset as atomic ..
    we can do it by storing offset in DB with processed data.
    exactly once will be achieved by this way.
     */
    public void consume() {
        KafkaConsumer<String, Stock> kafkaConsumer = getConfig();
        kafkaConsumer.subscribe(Arrays.asList("TOPIC_STOCK"), new StockListener(currentOffsets, kafkaConsumer));
        while (true) {
            ConsumerRecords<String, Stock> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Stock> cr : consumerRecords) {
                currentOffsets.put(new TopicPartition(cr.topic(),
                        cr.partition()), new
                        OffsetAndMetadata(cr.offset() + 1, "no metadata"));
                KafkaOffset kafkaOffset = new KafkaOffset.OffsetBuilder().topicName("STOCK_TOPIC")
                        .partition(cr.partition()).offSet(cr.offset() + 1).build();
                kafkaOffsetEntity.saveAndFlush(kafkaOffset);
            }
// commented as we committing the offset in database hence no need to set offset in kafka.
            // kafkaConsumer.commitAsync();
        }
    }

    // TODO
    @Transactional
    public void dbHelper() {


    }
}
