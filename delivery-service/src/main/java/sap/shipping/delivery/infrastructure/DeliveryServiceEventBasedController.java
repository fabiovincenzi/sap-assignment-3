package sap.shipping.delivery.infrastructure;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import sap.shipping.common.exagonal.Adapter;
import sap.shipping.common.kafka.InputEventChannel;
import sap.shipping.common.kafka.OutputEventChannel;
import sap.shipping.delivery.application.DeliveryService;
import sap.shipping.delivery.domain.Delivery;
import sap.shipping.delivery.domain.DeliveryId;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Inbound adapter of the delivery service, over event channels.
 *
 * Two kinds of message arrive here. A command is addressed to this service and expects an outcome,
 * so it has a request channel plus an approved and a rejected one, which carry what a status code
 * would carry, and a requestId tying each reply to its request. A fact is simply announced by
 * somebody else: it has no reply channel, and a failure to act on it is only recorded.
 */
@Adapter
public class DeliveryServiceEventBasedController extends AbstractVerticle {

    static Logger logger = Logger.getLogger("[Delivery Service Event-Based Controller]");

    /* static event channel names */

    /* a fact, not a request: the order service announces and does not wait for an outcome */
    static final String ORDER_CONFIRMED_EVC = "order-confirmed";

    static final String GET_DELIVERY_REQUESTS_EVC = "get-delivery-requests";
    static final String GET_DELIVERY_REQUESTS_APPROVED_EVC = "get-delivery-requests-approved";
    static final String GET_DELIVERY_REQUESTS_REJECTED_EVC = "get-delivery-requests-rejected";

    /* a report, not a request: the drone states where it is and expects no answer */
    static final String DRONE_POSITION_REPORTS_EVC = "drone-position-reports";

    /* the fleet answers an announced delivery on one of these two */
    static final String DRONE_ASSIGNED_EVC = "drone-assigned";
    static final String DRONE_UNAVAILABLE_EVC = "drone-unavailable";

    static final String CONSUMER_GROUP = "delivery-service";

    private final DeliveryService deliveryService;
    private final String evChannelsLocation;
    private final String consumerGroup;

    private InputEventChannel orderConfirmed;
    private InputEventChannel getDeliveryRequests;
    private InputEventChannel dronePositionReports;
    private InputEventChannel droneUnavailable;
    private InputEventChannel droneAssigned;
    
    private OutputEventChannel getDeliveryRequestsApproved;
    private OutputEventChannel getDeliveryRequestsRejected;

    public DeliveryServiceEventBasedController(DeliveryService deliveryService, String evChannelsLocation) {
        this(deliveryService, evChannelsLocation, CONSUMER_GROUP);
    }

    /* the group is a parameter so that an instance under test does not share it with a running
       one: consumers of the same group split the channels instead of each seeing them all */
    public DeliveryServiceEventBasedController(DeliveryService deliveryService, String evChannelsLocation,
                                               String consumerGroup) {
        this.deliveryService = deliveryService;
        this.evChannelsLocation = evChannelsLocation;
        this.consumerGroup = consumerGroup;
    }

    @Override
    public void start() {
        logger.log(Level.INFO, "Delivery Service event channels initializing...");

        // consumer
        orderConfirmed = input(ORDER_CONFIRMED_EVC);
        getDeliveryRequests = input(GET_DELIVERY_REQUESTS_EVC);
        dronePositionReports = input(DRONE_POSITION_REPORTS_EVC);
        droneAssigned = input(DRONE_ASSIGNED_EVC);
        droneUnavailable = input(DRONE_UNAVAILABLE_EVC);

        // consumer
        getDeliveryRequestsApproved = output(GET_DELIVERY_REQUESTS_APPROVED_EVC);
        getDeliveryRequestsRejected = output(GET_DELIVERY_REQUESTS_REJECTED_EVC);



        Future.all(List.of(
                orderConfirmed.init(this::scheduleDelivery),
                getDeliveryRequests.init(this::getDelivery),
                dronePositionReports.init(this::updateDronePosition),
                droneAssigned.init(this::assignDrone),
                droneUnavailable.init(this::noDroneAvailable)))
            .onSuccess(v -> logger.log(Level.INFO, "Delivery Service event channels ready"))
            .onFailure(err -> logger.log(Level.SEVERE, "Event channels unavailable - " + err.getMessage()));
    }

    private InputEventChannel input(String channel) {
        return new InputEventChannel(vertx, channel, evChannelsLocation, consumerGroup);
    }

    private OutputEventChannel output(String channel) {
        return new OutputEventChannel(vertx, channel, evChannelsLocation);
    }

    /**
     * Reacts to a confirmed order by scheduling its delivery. Nobody is waiting for an outcome,
     * so a rejection has no channel to travel on and is only recorded.
     *
     * The new delivery is not announced here: it is a fact the domain owns entirely, so the
     * observer registered on the service publishes it, whatever path led to the scheduling.
     */
    private void scheduleDelivery(JsonObject fact) {
        var orderId = fact.getString("orderId");
        logger.log(Level.INFO, "OrderConfirmed - scheduling the delivery of order " + orderId);
        try {
            deliveryService.scheduleDelivery(
                required(fact, "orderId"),
                requiredDouble(fact, "pickupLat"),
                requiredDouble(fact, "pickupLng"),
                requiredDouble(fact, "deliveryLat"),
                requiredDouble(fact, "deliveryLng"),
                requiredDouble(fact, "weightKg"));
        } catch (Exception e) {
            logger.log(Level.WARNING, "OrderConfirmed discarded for order " + orderId + " - " + e.getMessage());
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

    /** The fleet answered the announced delivery with a drone. */
    private void assignDrone(JsonObject answer) {
        try {
            var deliveryId = new DeliveryId(required(answer, "deliveryId"));
            deliveryService.assignDrone(deliveryId, required(answer, "droneId"));
        } catch (Exception e) {
            logger.log(Level.WARNING, "DroneAssigned discarded - " + e.getMessage());
        }
    }

    /**
     * The fleet answered that there is none. The delivery stays SCHEDULED, which is the
     * degraded state the availability scenario describes: nothing is rejected, the delivery
     * simply waits.
     */
    private void noDroneAvailable(JsonObject answer) {
        logger.log(Level.WARNING, "No drone for delivery " + answer.getString("deliveryId")
            + " - " + answer.getString("reason"));
    }

    private void getDelivery(JsonObject request) {
        var requestId = request.getString("requestId");
        logger.log(Level.INFO, "GetDelivery request - " + requestId);
        try {
            /* one channel serves both lookups: by delivery id, or by the order it belongs to */
            var delivery = request.containsKey("orderId")
                ? deliveryService.findByOrderId(required(request, "orderId"))
                : deliveryService.trackDelivery(new DeliveryId(required(request, "deliveryId")));
            if (delivery.isPresent()) {
                approve(getDeliveryRequestsApproved, requestId, delivery.get());
            } else {
                reject(getDeliveryRequestsRejected, requestId, "delivery not found");
            }
        } catch (Exception e) {
            reject(getDeliveryRequestsRejected, requestId, e.getMessage());
        }
    }

    /**
     * A report has no reply channel: the drone states where it is and carries on. An unknown
     * drone is logged and dropped, because there is nobody waiting for an answer.
     */
    private void updateDronePosition(JsonObject report) {
        try {
            deliveryService.updateDronePosition(
                required(report, "droneId"),
                requiredDouble(report, "lat"),
                requiredDouble(report, "lng"));
        } catch (Exception e) {
            logger.log(Level.WARNING, "DronePosition report discarded - " + e.getMessage());
        }
    }

    /** The reply to the caller, keyed by requestId so that each caller finds its own. */
    private void approve(OutputEventChannel channel, String requestId, Delivery delivery) {
        channel.postEvent(requestId, deliveryToJson(delivery).put("requestId", requestId));
    }

    private void reject(OutputEventChannel channel, String requestId, String reason) {
        logger.log(Level.WARNING, "Request " + requestId + " rejected - " + reason);
        channel.postEvent(requestId, new JsonObject().put("requestId", requestId).put("error", reason));
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
