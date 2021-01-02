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

    final Thread mainThread = Thread.currentThread();

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

        // Registering a shutdown hook so we can exit cleanly.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Starting exit...");
                // Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
                kafkaConsumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });



        while (true) {
            ConsumerRecords<String, Stock> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Stock> cr : consumerRecords) {
                currentOffsets.put(new TopicPartition(cr.topic(),
                        cr.partition()), new
                        OffsetAndMetadata(cr.offset() + 1, "no metadata"));


            }
            for (Map.Entry<TopicPartition, OffsetAndMetadata> es : currentOffsets.entrySet()) {

                KafkaOffset kafkaOffset = new KafkaOffset.OffsetBuilder().topicName(es.getKey().topic())
                        .partition(es.getKey().partition()).offSet(es.getValue().offset()).build();

                dbHelper(kafkaOffset);
            }


            log.info("Topic-partition-offset {}", currentOffsets);
// commented as we committing the offset in database hence no need to set offset in kafka.
            // kafkaConsumer.commitAsync();
        }
    }

    // TODO
    @Transactional
    public void dbHelper(KafkaOffset kafkaOffset) {
        log.info("DAO call Save and Flush {} ", kafkaOffset);
        kafkaOffsetEntity.saveAndFlush(kafkaOffset);
    }




}
