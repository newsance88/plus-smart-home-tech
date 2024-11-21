package service.sensor;

import config.KafkaConfig;
import config.KafkaEventProducer;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import sensor.LightSensorEvent;
import sensor.SensorEvent;
import sensor.SensorEventType;
import service.handler.BaseEventHandler;

@Service
public class LightSensorEventHandler extends BaseEventHandler<LightSensorAvro> {

    public LightSensorEventHandler(KafkaConfig config, KafkaEventProducer producer) {
        super(config, producer);
    }

    @Override
    protected LightSensorAvro mapToAvro(SensorEvent event) {
        var lightEvent = (LightSensorEvent) event;
        return new LightSensorAvro(
                lightEvent.getLinkQuality(),
                lightEvent.getLuminosity()
        );
    }

    @Override
    public SensorEventType getMessageType() {
        return SensorEventType.LIGHT_SENSOR_EVENT;
    }
}