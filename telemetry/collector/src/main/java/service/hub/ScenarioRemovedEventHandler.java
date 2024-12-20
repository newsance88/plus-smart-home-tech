package service.hub;

import config.KafkaConfig;
import config.KafkaEventProducer;
import hub.HubEvent;
import hub.HubEventType;
import hub.ScenarioRemovedEvent;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import service.handler.BaseHubHandler;

@Service
public class ScenarioRemovedEventHandler extends BaseHubHandler<ScenarioRemovedEventAvro> {
    public ScenarioRemovedEventHandler(KafkaConfig config, KafkaEventProducer producer) {
        super(config, producer);
    }

    @Override
    protected ScenarioRemovedEventAvro mapToAvro(HubEvent event) {
        var scenarioRemovedEvent = (ScenarioRemovedEvent) event;

        return new ScenarioRemovedEventAvro(scenarioRemovedEvent.getName());
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.SCENARIO_REMOVED;
    }
}
