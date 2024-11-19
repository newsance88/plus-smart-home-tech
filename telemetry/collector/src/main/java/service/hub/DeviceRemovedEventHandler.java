package service.hub;

import config.KafkaConfig;
import config.KafkaEventProducer;
import hub.DeviceRemovedEvent;
import hub.HubEvent;
import hub.HubEventType;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import service.handler.BaseHubHandler;

@Service
public class DeviceRemovedEventHandler extends BaseHubHandler<DeviceRemovedEventAvro> {
    public DeviceRemovedEventHandler(KafkaConfig config, KafkaEventProducer producer) {
        super(config, producer);
    }

    @Override
    protected DeviceRemovedEventAvro mapToAvro(HubEvent event) {
        var deviceRemovedEvent = (DeviceRemovedEvent) event;
        return new DeviceRemovedEventAvro(deviceRemovedEvent.getId());
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.DEVICE_REMOVED;
    }

}
