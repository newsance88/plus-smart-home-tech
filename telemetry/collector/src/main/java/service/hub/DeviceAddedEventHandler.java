package service.hub;

import config.KafkaConfig;
import config.KafkaEventProducer;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import service.handler.BaseHubHandler;

@Service
public class DeviceAddedEventHandler extends BaseHubHandler<DeviceAddedEventAvro> {

    public DeviceAddedEventHandler(KafkaConfig config, KafkaEventProducer producer) {
        super(config, producer);
    }


    @Override
    protected DeviceAddedEventAvro mapToAvro(HubEventProto event) {
        var deviceAddedEvent = event.getDeviceAdded();

        return new DeviceAddedEventAvro(
                deviceAddedEvent.getId(),
                DeviceTypeAvro.valueOf(deviceAddedEvent.getType().name())
        );
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }
}
