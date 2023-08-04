package dh.homework.consumer.service;

import dh.homework.consumer.domain.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import static dh.homework.consumer.config.KafkaConsumerConfig.PERSON_TOPIC_NAME;

@Service
@Slf4j
public class PersonListenerService {

    @KafkaListener(topics = PERSON_TOPIC_NAME, containerFactory = "personListenerContainerFactory")
    public void consume(@Payload Person person,
                        @Header(KafkaHeaders.RECEIVED_KEY) ConsumerRecord<String, Person> consumerRecord,
                        @Header(KafkaHeaders.GROUP_ID) String groupId) {
        log.info("""
                        {} consume new person {} : {}
                        Offset: {}
                        Partition: {}
                        """,
                groupId, consumerRecord.key(), person, consumerRecord.offset(), consumerRecord.partition());
    }


}
