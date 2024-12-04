package service;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import service.handler.HubHandler;
import service.handler.SensorHandler;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@GrpcService
public class EventController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final Map<HubEventProto.PayloadCase, HubHandler> hubHandlers;
    private final Map<SensorEventProto.PayloadCase, SensorHandler> sensorHandlers;

    public EventController(Set<HubHandler> hubEventHandlerSet,
                           Set<SensorHandler> sensorEventHandlerSet) {
        this.hubHandlers = hubEventHandlerSet.stream()
                .collect(Collectors.toMap(HubHandler::getMessageType, Function.identity()));
        this.sensorHandlers = sensorEventHandlerSet.stream()
                .collect(Collectors.toMap(SensorHandler::getMessageType, Function.identity()));
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            hubHandlers.get(request.getPayloadCase()).handle(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            sensorHandlers.get(request.getPayloadCase()).handle(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }
}
