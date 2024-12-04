package ru.yandex.practicum;

import org.springframework.stereotype.Repository;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Repository
public class SnapRepo {

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        SensorsSnapshotAvro newSnapshot;

        if (snapshots.containsKey(event.getHubId())) {
            newSnapshot = snapshots.get(event.getHubId());
        } else {
            newSnapshot = new SensorsSnapshotAvro();
            newSnapshot.setTimestamp(Instant.now());
            newSnapshot.setHubId(event.getHubId());
            newSnapshot.setSensorsState(new HashMap<>());

        }

        if (newSnapshot.getSensorsState().get(event.getId()) != null) {
            SensorStateAvro oldState = newSnapshot.getSensorsState().get(event.getId());
            if ((oldState.getTimestamp().isBefore(event.getTimestamp())
                    || oldState.getTimestamp().equals(event.getTimestamp()))
                    && oldState.getData().equals(event.getPayload()))
                return Optional.empty();
        }

        SensorStateAvro sensorStateAvro = new SensorStateAvro(event.getTimestamp(), event.getPayload());
        newSnapshot.getSensorsState().put(event.getId(), sensorStateAvro);
        newSnapshot.setTimestamp(event.getTimestamp());
        snapshots.put(newSnapshot.getHubId(), newSnapshot);
        return Optional.of(newSnapshot);
    }
}
