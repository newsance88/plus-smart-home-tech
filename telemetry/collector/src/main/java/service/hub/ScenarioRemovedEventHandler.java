package service.hub;

import config.KafkaConfig;
import config.KafkaEventProducer;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import service.handler.BaseHubHandler;

@Service
public class ScenarioRemovedEventHandler extends BaseHubHandler<ScenarioRemovedEventAvro> {
    public ScenarioRemovedEventHandler(KafkaConfig config, KafkaEventProducer producer) {
        super(config, producer);
    }

    @Override
    protected ScenarioRemovedEventAvro mapToAvro(HubEventProto event) {
        var scenarioRemovedEvent = event.getScenarioRemoved();

        return new ScenarioRemovedEventAvro(scenarioRemovedEvent.getName());
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_REMOVED;
    }
}
