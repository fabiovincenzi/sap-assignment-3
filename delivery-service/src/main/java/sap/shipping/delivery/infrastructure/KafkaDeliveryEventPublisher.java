package sap.shipping.delivery.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import sap.shipping.common.ddd.DomainEvent;
import sap.shipping.common.exagonal.Adapter;
import sap.shipping.common.kafka.OutputEventChannel;
import sap.shipping.delivery.application.DeliveryEventPublisher;
import sap.shipping.delivery.domain.DeliveryId;
import sap.shipping.delivery.domain.events.DeliveryCompleted;
import sap.shipping.delivery.domain.events.DeliveryFailed;
import sap.shipping.delivery.domain.events.DeliveryScheduled;
import sap.shipping.delivery.domain.events.DroneAssigned;
import sap.shipping.delivery.domain.events.DronePositionUpdated;
import sap.shipping.delivery.domain.events.TransitStarted;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Publishes the domain events of a delivery on delivery-{deliveryId}-events, created the first
 * time that delivery produces one. The channels are cached: a producer holds a connection.
 */
@Adapter
public class KafkaDeliveryEventPublisher implements DeliveryEventPublisher {

    private final Vertx vertx;
    private final String evChannelsLocation;
    private final Map<String, OutputEventChannel> channels = new ConcurrentHashMap<>();

    public KafkaDeliveryEventPublisher(Vertx vertx, String evChannelsLocation) {
        this.vertx = vertx;
        this.evChannelsLocation = evChannelsLocation;
    }

    @Override
    public void publish(DeliveryId deliveryId, DomainEvent event) {
        var id = deliveryId.value();
        var channel = channels.computeIfAbsent(id,
            key -> new OutputEventChannel(vertx, "delivery-" + key + "-events", evChannelsLocation));
        channel.postEvent(id, toJson(deliveryId, event));
    }

    private JsonObject toJson(DeliveryId deliveryId, DomainEvent event) {
        var json = new JsonObject().put("deliveryId", deliveryId.value());
        if (event instanceof DeliveryScheduled e) {
            json.put("event", "delivery-scheduled")
                .put("orderId", e.orderId())
                .put("weightKg", e.weightKg())
                .put("createdAt", e.createdAt().toString());
        } else if (event instanceof DroneAssigned e) {
            json.put("event", "drone-assigned").put("droneId", e.droneId());
        } else if (event instanceof TransitStarted e) {
            json.put("event", "transit-started");
        } else if (event instanceof DronePositionUpdated e) {
            json.put("event", "drone-position-updated").put("lat", e.lat()).put("lng", e.lng());
        } else if (event instanceof DeliveryCompleted e) {
            json.put("event", "delivery-completed")
                .put("orderId", e.orderId())
                .put("droneId", e.droneId());
        } else if (event instanceof DeliveryFailed e) {
            json.put("event", "delivery-failed")
                .put("orderId", e.orderId())
                .put("reason", e.reason());
        } else {
            throw new IllegalArgumentException("Unknown event type: " + event.getClass().getName());
        }
        return json;
    }
}
