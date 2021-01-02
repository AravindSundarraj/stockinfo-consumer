package com.group.kaka.stockinfoconsumer.callback;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

@Component
@Slf4j
public class OffsetCommitCallbackImpl implements OffsetCommitCallback {
    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
        log.info("Stock consumer Async Commit callback ");
        if(Objects.nonNull(e))
            log.error("Exception occurred in stock -async call back  - commit failed {} " , e);

    }
}
