package service.sensor;

import config.KafkaConfig;
import config.KafkaEventProducer;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import service.handler.BaseEventHandler;

public class TemperatureSensorEventHandler extends BaseEventHandler<TemperatureSensorAvro> {
    public TemperatureSensorEventHandler(KafkaConfig config, KafkaEventProducer producer) {
        super(config, producer);
    }

    @Override
    protected TemperatureSensorAvro mapToAvro(SensorEventProto event) {
        var tempEvent = event.getTemperatureSensorEvent();

        return new TemperatureSensorAvro(
                event.getId(),
                event.getHubId(),
                event.getTimestamp().getSeconds(),
                tempEvent.getTemperatureC(),
                tempEvent.getTemperatureF()
        );
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR_EVENT;
    }

}
