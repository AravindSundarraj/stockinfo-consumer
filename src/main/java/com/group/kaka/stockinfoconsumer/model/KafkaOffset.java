package com.group.kaka.stockinfoconsumer.model;

import com.group.kaka.stockinfoconsumer.exception.OffSetException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;




@Entity
public class KafkaOffset {
    private String topicName;
    private int partition;
    private long offSet;

    private KafkaOffset(OffsetBuilder offsetBuilder){
        this.topicName = offsetBuilder.topicNameBuilder;
        this.partition = offsetBuilder.partitionBuilder;
        this.offSet =  offsetBuilder.offsetBuilder;

    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffSet() {
        return offSet;
    }

    public static class OffsetBuilder{
        private String topicNameBuilder;
        private int partitionBuilder;
        private long offsetBuilder;



        public OffsetBuilder topicName(String topicNameBuilder){
            this.topicNameBuilder = topicNameBuilder;
            return this;
        }
        public OffsetBuilder partition(int partitionBuilder){
            this.partitionBuilder = partitionBuilder;
            return this;
        }

        public OffsetBuilder offSet(long offsetBuilder){
            this.offsetBuilder = offsetBuilder;
            return this;
        }

        public KafkaOffset build(){
            if(this.topicNameBuilder == null){
                throw new OffSetException("Topic Name cannot be empty !!! ");
            }
            if(this.partitionBuilder < 1){
                throw new OffSetException("Partition Number cannot be less than 1 !!! ");
            }
            if(this.offsetBuilder < 1){
                throw new OffSetException("Offset Number cannot be less than 1 !!! ");
            }

            KafkaOffset KafkaOffset = new KafkaOffset(this);
            return KafkaOffset;
        }
    }
}
