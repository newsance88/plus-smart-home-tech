package config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaEventProducer {
    private final KafkaProducer<String, SpecificRecordBase> kafkaProducer;

    public <T extends SpecificRecordBase> void send(String topic, String key, T event) {

        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(topic, key, event);
        log.info("Sending event to Kafka | Topic: {} | Key: {} | Event: {}",
                topic, key, event);
        kafkaProducer.send(record);
    }
}
