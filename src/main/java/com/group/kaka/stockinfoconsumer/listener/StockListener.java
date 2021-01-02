package com.group.kaka.stockinfoconsumer.listener;

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
    KafkaOffsetEntity kafkaOffsetEntity;
   private Map<TopicPartition, OffsetAndMetadata> currentOffsets;
   private Consumer consumer;
    public StockListener(Map<TopicPartition, OffsetAndMetadata> currentOffsets , Consumer consumer){
        this.currentOffsets = currentOffsets;
        this.consumer = consumer;
    }
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

        log.info("Rebalance onPartitionsRevoked Trigger ... commit the Offset ...");
        kafkaOffsetEntity.saveAndFlush();

        for(TopicPartition partition: collection){

        }

        consumer.commitSync(currentOffsets);

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        log.info("Rebalance onPartitionsAssigned Trigger ... Get the current Offset ...");
        for(TopicPartition partition: collection){
            log.info("The partition ID : {} " , partition.partition());
           Optional<KafkaOffset> op = kafkaOffsetEntity.findById(partition.topic());
            consumer.seek(partition , op.get().getOffSet());
        }
    }
}
