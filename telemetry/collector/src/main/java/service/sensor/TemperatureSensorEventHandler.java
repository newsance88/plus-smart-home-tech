package service.sensor;

import config.KafkaConfig;
import config.KafkaEventProducer;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import sensor.SensorEvent;
import sensor.SensorEventType;
import sensor.TemperatureSensorEvent;
import service.handler.BaseEventHandler;

public class TemperatureSensorEventHandler extends BaseEventHandler<TemperatureSensorAvro> {
    public TemperatureSensorEventHandler(KafkaConfig config, KafkaEventProducer producer) {
        super(config, producer);
    }

    @Override
    protected TemperatureSensorAvro mapToAvro(SensorEvent event) {
        var tempEvent = (TemperatureSensorEvent) event;

        return new TemperatureSensorAvro(
                tempEvent.getId(),
                tempEvent.getHubId(),
                tempEvent.getTimestamp(),
                tempEvent.getTemperatureC(),
                tempEvent.getTemperatureF()
        );
    }

    @Override
    public SensorEventType getMessageType() {
        return SensorEventType.TEMPERATURE_SENSOR_EVENT;
    }

}
