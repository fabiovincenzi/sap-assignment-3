package sap.shipping.delivery.infrastructure;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import sap.shipping.common.exagonal.Adapter;
import sap.shipping.common.kafka.InputEventChannel;
import sap.shipping.common.kafka.OutputEventChannel;
import sap.shipping.delivery.application.DeliveryService;
import sap.shipping.delivery.domain.Delivery;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Event-based controller of the delivery service: the same application service the REST
 * controller drives, reached through event channels instead of HTTP routes.
 *
 * A request channel carries the command, the approved and rejected channels carry what an
 * HTTP status code would carry, and the requestId correlates a reply with its request.
 */
@Adapter
public class DeliveryServiceEventBasedController extends AbstractVerticle {

    static Logger logger = Logger.getLogger("[Delivery Service Event-Based Controller]");

    /* static event channel names */

    static final String CREATE_DELIVERY_REQUESTS_EVC = "create-delivery-requests";
    static final String CREATE_DELIVERY_REQUESTS_APPROVED_EVC = "create-delivery-requests-approved";
    static final String CREATE_DELIVERY_REQUESTS_REJECTED_EVC = "create-delivery-requests-rejected";

    static final String NEW_DELIVERY_CREATED_EVC = "new-delivery-created";

    static final String CONSUMER_GROUP = "delivery-service";

    private final DeliveryService deliveryService;
    private final String evChannelsLocation;

    private InputEventChannel createDeliveryRequests;
    private OutputEventChannel createDeliveryRequestsApproved;
    private OutputEventChannel createDeliveryRequestsRejected;
    private OutputEventChannel newDeliveryCreated;

    public DeliveryServiceEventBasedController(DeliveryService deliveryService, String evChannelsLocation) {
        this.deliveryService = deliveryService;
        this.evChannelsLocation = evChannelsLocation;
    }

    @Override
    public void start() {
        logger.log(Level.INFO, "Delivery Service event channels initializing...");

        createDeliveryRequests =
            new InputEventChannel(vertx, CREATE_DELIVERY_REQUESTS_EVC, evChannelsLocation, CONSUMER_GROUP);
        createDeliveryRequestsApproved =
            new OutputEventChannel(vertx, CREATE_DELIVERY_REQUESTS_APPROVED_EVC, evChannelsLocation);
        createDeliveryRequestsRejected =
            new OutputEventChannel(vertx, CREATE_DELIVERY_REQUESTS_REJECTED_EVC, evChannelsLocation);
        newDeliveryCreated =
            new OutputEventChannel(vertx, NEW_DELIVERY_CREATED_EVC, evChannelsLocation);

        createDeliveryRequests.init(this::scheduleDelivery)
            .onSuccess(v -> logger.log(Level.INFO, "Delivery Service event channels ready"))
            .onFailure(err -> logger.log(Level.SEVERE, "Event channels unavailable - " + err.getMessage()));
    }

    private void scheduleDelivery(JsonObject request) {
        var requestId = request.getString("requestId");
        logger.log(Level.INFO, "ScheduleDelivery request - " + requestId);
        try {
            var delivery = deliveryService.scheduleDelivery(
                required(request, "orderId"),
                requiredDouble(request, "pickupLat"),
                requiredDouble(request, "pickupLng"),
                requiredDouble(request, "deliveryLat"),
                requiredDouble(request, "deliveryLng"),
                requiredDouble(request, "weightKg"));

            approve(requestId, delivery);
            announce(delivery);
        } catch (Exception e) {
            reject(requestId, e.getMessage());
        }
    }

    /* an event carries no schema, so a missing field must be reported as such and not
       surface as a null dereference in the reply */
    private static String required(JsonObject event, String field) {
        var value = event.getString(field);
        if (value == null) {
            throw new IllegalArgumentException("missing field: " + field);
        }
        return value;
    }

    private static double requiredDouble(JsonObject event, String field) {
        var value = event.getDouble(field);
        if (value == null) {
            throw new IllegalArgumentException("missing or not numeric field: " + field);
        }
        return value;
    }

    /** The reply to the caller, keyed by requestId so that each caller finds its own. */
    private void approve(String requestId, Delivery delivery) {
        var reply = deliveryToJson(delivery).put("requestId", requestId);
        createDeliveryRequestsApproved.postEvent(requestId, reply);
    }

    private void reject(String requestId, String reason) {
        logger.log(Level.WARNING, "ScheduleDelivery rejected - " + reason);
        var reply = new JsonObject().put("requestId", requestId).put("error", reason);
        createDeliveryRequestsRejected.postEvent(requestId, reply);
    }

    /**
     * The fact, addressed to nobody in particular: whoever is interested subscribes, and the
     * delivery service does not need to know who.
     */
    private void announce(Delivery delivery) {
        newDeliveryCreated.postEvent(delivery.getId().value(), deliveryToJson(delivery));
    }

    private JsonObject deliveryToJson(Delivery d) {
        return new JsonObject()
            .put("id", d.getId().value())
            .put("orderId", d.orderId())
            .put("status", d.status().name())
            .put("droneId", d.droneId())
            .put("currentLat", d.currentLat())
            .put("currentLng", d.currentLng())
            .put("estimatedMinutes", d.estimatedMinutes())
            .put("createdAt", d.createdAt().toString());
    }
}
