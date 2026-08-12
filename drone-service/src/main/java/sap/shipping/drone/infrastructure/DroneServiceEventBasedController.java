package sap.shipping.drone.infrastructure;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import sap.shipping.common.exagonal.Adapter;
import sap.shipping.common.kafka.InputEventChannel;
import sap.shipping.common.kafka.OutputEventChannel;
import sap.shipping.drone.application.DroneService;
import sap.shipping.drone.domain.DroneId;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reacts to an announced delivery by reserving a drone, and answers on one of two channels.
 * Without the unavailable one, a waiting delivery could not tell a slow answer from no answer.
 */
@Adapter
public class DroneServiceEventBasedController extends AbstractVerticle {

    static Logger logger = Logger.getLogger("[Drone Service Event-Based Controller]");

    static final String NEW_DELIVERY_CREATED_EVC = "new-delivery-created";
    static final String DELIVERY_COMPLETED_EVC = "delivery-completed";

    static final String DRONE_ASSIGNED_EVC = "drone-assigned";
    static final String DRONE_UNAVAILABLE_EVC = "drone-unavailable";

    static final String CONSUMER_GROUP = "drone-service";

    private final DroneService droneService;
    private final String evChannelsLocation;

    private InputEventChannel newDeliveryCreated;
    private InputEventChannel deliveryCompleted;
    private OutputEventChannel droneAssigned;
    private OutputEventChannel droneUnavailable;

    public DroneServiceEventBasedController(DroneService droneService, String evChannelsLocation) {
        this.droneService = droneService;
        this.evChannelsLocation = evChannelsLocation;
    }

    @Override
    public void start() {
        logger.log(Level.INFO, "Drone Service event channels initializing...");

        newDeliveryCreated =
            new InputEventChannel(vertx, NEW_DELIVERY_CREATED_EVC, evChannelsLocation, CONSUMER_GROUP);
        deliveryCompleted =
            new InputEventChannel(vertx, DELIVERY_COMPLETED_EVC, evChannelsLocation, CONSUMER_GROUP);
        droneAssigned = new OutputEventChannel(vertx, DRONE_ASSIGNED_EVC, evChannelsLocation);
        droneUnavailable = new OutputEventChannel(vertx, DRONE_UNAVAILABLE_EVC, evChannelsLocation);

        Future.all(List.of(
                newDeliveryCreated.init(this::reserveDroneFor),
                deliveryCompleted.init(this::releaseDroneOf)))
            .onSuccess(v -> logger.log(Level.INFO, "Drone Service event channels ready"))
            .onFailure(err -> logger.log(Level.SEVERE, "Event channels unavailable - " + err.getMessage()));
    }

    private void reserveDroneFor(JsonObject fact) {
        var deliveryId = fact.getString("deliveryId");
        logger.log(Level.INFO, "NewDeliveryCreated - looking for a drone for " + deliveryId);
        try {
            var drone = droneService.findAvailableDrone(
                fact.getDouble("pickupLat"), fact.getDouble("pickupLng"), fact.getDouble("weightKg"));
            if (drone.isEmpty()) {
                announceUnavailable(deliveryId, "no drone available for the requested weight");
                return;
            }
            droneService.assignDrone(drone.get().getId());
            droneAssigned.postEvent(deliveryId, new JsonObject()
                .put("deliveryId", deliveryId)
                .put("droneId", drone.get().getId().value()));
        } catch (Exception e) {
            announceUnavailable(deliveryId, e.getMessage());
        }
    }

    private void releaseDroneOf(JsonObject fact) {
        var droneId = fact.getString("droneId");
        if (droneId == null) {
            return;
        }
        logger.log(Level.INFO, "DeliveryCompleted - releasing drone " + droneId);
        try {
            droneService.releaseDrone(new DroneId(droneId));
        } catch (Exception e) {
            logger.log(Level.WARNING, "Release of drone " + droneId + " failed - " + e.getMessage());
        }
    }

    private void announceUnavailable(String deliveryId, String reason) {
        logger.log(Level.INFO, "No drone for delivery " + deliveryId + " - " + reason);
        droneUnavailable.postEvent(deliveryId, new JsonObject()
            .put("deliveryId", deliveryId)
            .put("reason", reason));
    }
}
