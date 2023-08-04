package dh.homework.producer.service;

import dh.homework.producer.domain.Person;
import dh.homework.producer.service.callbackListeners.PersonListenerCallback;
import dh.homework.producer.service.callbackListeners.StringListenerCallback;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import static dh.homework.producer.config.KafkaProducerConfig.PERSON_TOPIC_NAME;
import static dh.homework.producer.config.KafkaProducerConfig.TOPIC_NAME;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)

public class SenderService {

    KafkaTemplate<String, String> kafkaTemplate;
    KafkaTemplate<String, Person> personKafkaTemplate;
    StringListenerCallback stringListenerCallback;
    PersonListenerCallback personListenerCallback;

    public void sendMessage(String key, String message) {
        val future = kafkaTemplate.send(TOPIC_NAME, key, message);
        future.whenComplete(stringListenerCallback);
    }

    public void sendPersonMessage(Person person) {
        val future = personKafkaTemplate.send(PERSON_TOPIC_NAME, person.getId(), person);
        future.whenComplete(personListenerCallback);
    }
}
