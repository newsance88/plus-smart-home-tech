package service.sensor;

import config.KafkaConfig;
import config.KafkaEventProducer;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import service.handler.BaseEventHandler;

@Service
public class MotionSensorEventHandler extends BaseEventHandler<MotionSensorAvro> {
    public MotionSensorEventHandler(KafkaConfig config, KafkaEventProducer producer) {
        super(config, producer);
    }

    @Override
    protected MotionSensorAvro mapToAvro(SensorEventProto event) {
        var motionEvent = event.getMotionSensorEvent();

        return new MotionSensorAvro(
                motionEvent.getLinkQuality(),
                motionEvent.getMotion(),
                motionEvent.getVoltage()
        );
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR_EVENT;
    }
}
