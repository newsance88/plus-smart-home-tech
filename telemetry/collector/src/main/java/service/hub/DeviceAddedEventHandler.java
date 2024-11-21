package service.hub;

import config.KafkaConfig;
import config.KafkaEventProducer;
import hub.DeviceAddedEvent;
import hub.HubEvent;
import hub.HubEventType;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import service.handler.BaseHubHandler;

@Service
public class DeviceAddedEventHandler extends BaseHubHandler<DeviceAddedEventAvro> {

    public DeviceAddedEventHandler(KafkaConfig config, KafkaEventProducer producer) {
        super(config, producer);
    }


    @Override
    protected DeviceAddedEventAvro mapToAvro(HubEvent event) {
        var deviceAddedEvent = (DeviceAddedEvent) event;

        return new DeviceAddedEventAvro(
                deviceAddedEvent.getId(),
                DeviceTypeAvro.valueOf(deviceAddedEvent.getDeviceType().name())
        );
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.DEVICE_ADDED;
    }
}
