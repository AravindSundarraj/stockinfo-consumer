package com.group.kaka.stockinfoconsumer.listener;

import com.group.kaka.stockinfoconsumer.consumer.StockConsumer;
import com.group.kaka.stockinfoconsumer.dao.KafkaOffsetEntity;
import com.group.kaka.stockinfoconsumer.model.KafkaOffset;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

@Component
@Slf4j
public class StockListener implements ConsumerRebalanceListener {

    @Autowired
    StockConsumer stockConsumer;
    @Autowired
    KafkaOffsetEntity kafkaOffsetEntity;

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    private Consumer consumer;

    public StockListener(Map<TopicPartition, OffsetAndMetadata> currentOffsets, Consumer consumer) {
        this.currentOffsets = currentOffsets;
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

        log.info("Rebalance onPartitionsRevoked Trigger ... commit the Offset ...");


        for (TopicPartition partition : collection) {

            OffsetAndMetadata offsetAndMetadata = currentOffsets.get(partition);

            KafkaOffset kafkaOffset = new KafkaOffset.OffsetBuilder()
                    .topicName(partition.topic()).partition(partition.partition()).offSet(offsetAndMetadata.offset()).build();
            stockConsumer.dbHelper(kafkaOffset);
        }

        //  consumer.commitSync(currentOffsets);

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        log.info("Rebalance onPartitionsAssigned Trigger ... Get the current Offset ...");
        for (TopicPartition partition : collection) {
            log.info("The partition ID : {} ", partition.partition());
            Optional<KafkaOffset> op = kafkaOffsetEntity.findById(partition.topic());
            consumer.seek(partition, op.get().getOffSet());
        }
    }
}
