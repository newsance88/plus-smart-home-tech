package service.handler;

import config.KafkaConfig;
import config.KafkaEventProducer;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;

@Component
@RequiredArgsConstructor
public abstract class BaseHubHandler<T extends SpecificRecordBase> implements HubHandler {
    private final KafkaConfig config;
    private final KafkaEventProducer producer;
    private static final String HUB_TOPIC = "telemetry.hubs.v1";

    protected abstract T mapToAvro(HubEventProto event);

    public void handle(HubEventProto event) {
        T avroEvent = mapToAvro(event);
        producer.send(HUB_TOPIC, event.getHubId(), avroEvent);
    }
}
