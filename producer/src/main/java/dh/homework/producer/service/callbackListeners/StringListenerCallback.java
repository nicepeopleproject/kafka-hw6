package dh.homework.producer.service.callbackListeners;

import java.util.function.BiConsumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class StringListenerCallback implements BiConsumer<SendResult<String, String>, Throwable> {

    @Override
    public void accept(SendResult<String, String> stringStringSendResult, Throwable throwable) {
        if (throwable != null) {
            log.error("Cannot send message to Kafka: ", throwable);
        } else {
            log.info("""
                            Send message to Kafka: 
                            Offset: {}
                            Partition: {}
                            """,
                    stringStringSendResult.getRecordMetadata().offset(),
                    stringStringSendResult.getRecordMetadata().partition());
        }
    }
}
