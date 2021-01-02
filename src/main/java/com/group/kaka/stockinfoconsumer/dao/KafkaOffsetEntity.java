package com.group.kaka.stockinfoconsumer.dao;

import com.group.kaka.stockinfoconsumer.model.KafkaOffset;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface KafkaOffsetEntity extends JpaRepository<KafkaOffset , String> {
}
