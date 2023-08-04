package dh.homework.producer.service.callbackListeners;

import dh.homework.producer.domain.Person;

import java.util.function.BiConsumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PersonListenerCallback implements BiConsumer<SendResult<String, Person>, Throwable> {

    @Override
    public void accept(SendResult<String, Person> stringStringSendResult, Throwable throwable) {
        if (throwable != null) {
            log.error("Cannot send message to Kafka: ", throwable);
        } else {
            log.info("""
                            Send message to Kafka: +
                            Offset: {} +
                            Partition: {}
                            """,
                    stringStringSendResult.getRecordMetadata().offset(),
                    stringStringSendResult.getRecordMetadata().partition());
        }
    }
}
