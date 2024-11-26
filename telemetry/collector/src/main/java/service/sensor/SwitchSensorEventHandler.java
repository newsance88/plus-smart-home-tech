package service.sensor;

import config.KafkaConfig;
import config.KafkaEventProducer;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import sensor.SensorEvent;
import sensor.SensorEventType;
import sensor.SwitchSensorEvent;
import service.handler.BaseEventHandler;

@Service
public class SwitchSensorEventHandler extends BaseEventHandler<SwitchSensorAvro> {
    public SwitchSensorEventHandler(KafkaConfig config, KafkaEventProducer producer) {
        super(config, producer);
    }

    @Override
    protected SwitchSensorAvro mapToAvro(SensorEvent event) {
        var switchEvent = (SwitchSensorEvent) event;

        return new SwitchSensorAvro(switchEvent.isState());
    }

    @Override
    public SensorEventType getMessageType() {
        return SensorEventType.SWITCH_SENSOR_EVENT;
    }

}
