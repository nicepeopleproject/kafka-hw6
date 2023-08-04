package dh.homework.consumer.service;

import dh.homework.consumer.config.KafkaConsumerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ListenerService {

    @KafkaListener(topics = KafkaConsumerConfig.TOPIC_NAME)
    public void consumeFirstGroup(@Payload String message,
                        @Header(KafkaHeaders.RECEIVED_KEY) ConsumerRecord<String, String> consumerRecord,
                        @Header(KafkaHeaders.GROUP_ID) String groupId) {
        log.info("""
                {} consume new message {} : {}
                Offset: {}
                Partition: {}
                """, groupId, consumerRecord.key(), message, consumerRecord.offset(), consumerRecord.partition());
    }

    @KafkaListener(topics = KafkaConsumerConfig.TOPIC_NAME,
            groupId = "group-2")
    public void consumeSecondGroup(@Payload String message,
                                        @Header(KafkaHeaders.RECEIVED_KEY) ConsumerRecord<String, String> consumerRecord,
                                        @Header(KafkaHeaders.GROUP_ID) String groupId) {
        log.info("""
                {} consume new message {} : {} +
                Offset: {} +
                Partition: {}
                """, groupId, consumerRecord.key(), message, consumerRecord.offset(), consumerRecord.partition());
    }


    @KafkaListener(topicPartitions = @TopicPartition(topic = KafkaConsumerConfig.TOPIC_NAME,
            partitionOffsets = {
                    @PartitionOffset(partition = "0", initialOffset = "0"),
                    @PartitionOffset(partition = "1", initialOffset = "0"),}),
            groupId = "group-4")
    public void consumeAllMessages(@Payload String message,
                                  @Header(KafkaHeaders.RECEIVED_KEY) ConsumerRecord<String, String> consumerRecord,
                                  @Header(KafkaHeaders.GROUP_ID) String groupId) {
        log.info("""
                {} consume new message {} : {} +
                Offset: {} +
                Partition: {}
                """, groupId, consumerRecord.key(), message, consumerRecord.offset(), consumerRecord.partition());
    }


    @KafkaListener(topicPartitions = @TopicPartition(topic = KafkaConsumerConfig.TOPIC_NAME,
            partitions = {"1", "2"}),
            groupId = "group-3")
    public void consumeSomePartitions(@Payload String message,
                                      @Header(KafkaHeaders.RECEIVED_KEY) ConsumerRecord<String, String> consumerRecord,
                                      @Header(KafkaHeaders.GROUP_ID) String groupId) {
        log.info("""
                {} consume new message {} : {}
                Offset: {}
                Partition: {}
                """, groupId, consumerRecord.key(), message, consumerRecord.offset(), consumerRecord.partition());
    }

}
