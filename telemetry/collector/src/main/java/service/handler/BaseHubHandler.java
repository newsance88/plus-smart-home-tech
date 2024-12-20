package service.handler;

import config.KafkaConfig;
import config.KafkaEventProducer;
import hub.HubEvent;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public abstract class BaseHubHandler<T extends SpecificRecordBase> implements HubHandler {
    private final KafkaConfig config;
    private final KafkaEventProducer producer;
    private static final String HUB_TOPIC = "telemetry.hubs.v1";

    protected abstract T mapToAvro(HubEvent event);

    public void handle(HubEvent event) {
        T avroEvent = mapToAvro(event);
        producer.send(HUB_TOPIC, event.getHubId(), avroEvent);
    }
}
