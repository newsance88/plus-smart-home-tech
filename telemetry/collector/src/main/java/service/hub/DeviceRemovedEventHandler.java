package service.hub;

import config.KafkaConfig;
import config.KafkaEventProducer;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import service.handler.BaseHubHandler;

@Service
public class DeviceRemovedEventHandler extends BaseHubHandler<DeviceRemovedEventAvro> {
    public DeviceRemovedEventHandler(KafkaConfig config, KafkaEventProducer producer) {
        super(config, producer);
    }

    @Override
    protected DeviceRemovedEventAvro mapToAvro(HubEventProto event) {
        var deviceRemovedEvent = event.getDeviceRemoved();
        return new DeviceRemovedEventAvro(deviceRemovedEvent.getId());
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }

}
