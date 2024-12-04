package ru.yandex.practicum;

import lombok.Getter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.util.Properties;

@Configuration
@Getter
public class KafkaConfig {
    @Value("${collector.kafka.producer.properties.bootstrap-servers}")
    String bootstrapServer;
    @Value("${collector.kafka.producer.group.id}")
    String groupId;
    @Value("${collector.kafka.producer.topics.in}")
    String topicIn;
    @Value("${collector.kafka.producer.topics.out}")
    String topicOut;

    @Bean
    public KafkaProducer<String, SpecificRecordBase> getProducer() {
        Properties configProps = new Properties();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BaseAvroSerializer.class);

        return new KafkaProducer<>(configProps);
    }

    @Bean
    public KafkaConsumer<String, SensorEventAvro> getConsumer() {
        Properties configProps = new Properties();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventDeserializer.class);
        return new KafkaConsumer<>(configProps);
    }
}
