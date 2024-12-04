package service.sensor;

import config.KafkaConfig;
import config.KafkaEventProducer;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import service.handler.BaseEventHandler;

@Service
public class SwitchSensorEventHandler extends BaseEventHandler<SwitchSensorAvro> {
    public SwitchSensorEventHandler(KafkaConfig config, KafkaEventProducer producer) {
        super(config, producer);
    }

    @Override
    protected SwitchSensorAvro mapToAvro(SensorEventProto event) {
        var switchEvent = event.getSwitchSensorEvent();

        return new SwitchSensorAvro(switchEvent.getState());
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.SWITCH_SENSOR_EVENT;
    }

}
