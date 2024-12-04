package service.sensor;

import config.KafkaConfig;
import config.KafkaEventProducer;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import service.handler.BaseEventHandler;

@Service
public class LightSensorEventHandler extends BaseEventHandler<LightSensorAvro> {

    public LightSensorEventHandler(KafkaConfig config, KafkaEventProducer producer) {
        super(config, producer);
    }

    @Override
    protected LightSensorAvro mapToAvro(SensorEventProto event) {
        var lightEvent = event.getLightSensorEvent();
        return new LightSensorAvro(
                lightEvent.getLinkQuality(),
                lightEvent.getLuminosity()
        );
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR_EVENT;
    }
}