package service.handler;

import config.KafkaConfig;
import config.KafkaEventProducer;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

@RequiredArgsConstructor
@Component
public abstract class BaseEventHandler<T extends SpecificRecordBase> implements SensorHandler {

    private final KafkaConfig config;
    private final KafkaEventProducer producer;
    private static final String SENSOR_TOPIC = "telemetry.sensors.v1";

    protected abstract T mapToAvro(SensorEventProto event);

    public void handle(SensorEventProto event) {

        T protoEvent = mapToAvro(event);
        producer.send(SENSOR_TOPIC, event.getId(), protoEvent);
    }
}
