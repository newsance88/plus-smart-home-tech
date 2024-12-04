package ru.yandex.practicum;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

@RequiredArgsConstructor
@Component
@Slf4j
public class AggregationStarter {

    private final KafkaProducer<String, SpecificRecordBase> producer;
    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final SnapRepo snapshotRepository;
    private final KafkaConfig config;

    public void start() {
        try {
            consumer.subscribe(List.of(config.getTopicIn()));
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(5000));

                if (records.isEmpty())  {
                    continue;
                }

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    log.info("Сообщение {},СМещение {}", record.value(), record.offset());

                    Optional<SensorsSnapshotAvro> snapshotAvro = snapshotRepository.updateState(record.value());
                    if (snapshotAvro.isPresent()) {

                        ProducerRecord<String, SpecificRecordBase> producerRecord = new ProducerRecord<>(config.getTopicOut(), snapshotAvro.get().getHubId(), snapshotAvro.get());

                        producer.send(producerRecord, (recordMetadata, e) -> {
                            if (e != null) {
                                throw new RuntimeException("Ошибка отправки", e);
                            }
                            log.info("отправлено, topic {}", recordMetadata.topic());
                        });
                    }
                }
                consumer.commitSync();
            }

        } catch (WakeupException ignored) {

        } catch (Exception ex) {
            log.error("ошибка",ex);
        } finally {
            log.info("закрыли конс");
            consumer.close();
            log.info("Закрыли прод");
            producer.close();
        }
    }
}
