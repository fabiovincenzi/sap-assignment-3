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
 * Each operation has a request channel plus an approved and a rejected one, which carry what a
 * status code would carry. The requestId ties a reply to its request.
 */
@Adapter
public class DeliveryServiceEventBasedController extends AbstractVerticle {

    static Logger logger = Logger.getLogger("[Delivery Service Event-Based Controller]");

    /* static event channel names */

    static final String CREATE_DELIVERY_REQUESTS_EVC = "create-delivery-requests";
    static final String CREATE_DELIVERY_REQUESTS_APPROVED_EVC = "create-delivery-requests-approved";
    static final String CREATE_DELIVERY_REQUESTS_REJECTED_EVC = "create-delivery-requests-rejected";

    static final String START_DELIVERY_REQUESTS_EVC = "start-delivery-requests";
    static final String START_DELIVERY_REQUESTS_APPROVED_EVC = "start-delivery-requests-approved";
    static final String START_DELIVERY_REQUESTS_REJECTED_EVC = "start-delivery-requests-rejected";

    static final String COMPLETE_DELIVERY_REQUESTS_EVC = "complete-delivery-requests";
    static final String COMPLETE_DELIVERY_REQUESTS_APPROVED_EVC = "complete-delivery-requests-approved";
    static final String COMPLETE_DELIVERY_REQUESTS_REJECTED_EVC = "complete-delivery-requests-rejected";

    static final String GET_DELIVERY_REQUESTS_EVC = "get-delivery-requests";
    static final String GET_DELIVERY_REQUESTS_APPROVED_EVC = "get-delivery-requests-approved";
    static final String GET_DELIVERY_REQUESTS_REJECTED_EVC = "get-delivery-requests-rejected";

    /* a report, not a request: the drone states where it is and expects no answer */
    static final String DRONE_POSITION_REPORTS_EVC = "drone-position-reports";

    static final String NEW_DELIVERY_CREATED_EVC = "new-delivery-created";
    static final String DELIVERY_COMPLETED_EVC = "delivery-completed";

    /* the fleet answers an announced delivery on one of these two */
    static final String DRONE_ASSIGNED_EVC = "drone-assigned";
    static final String DRONE_UNAVAILABLE_EVC = "drone-unavailable";

    static final String CONSUMER_GROUP = "delivery-service";

    private final DeliveryService deliveryService;
    private final String evChannelsLocation;

    private InputEventChannel createDeliveryRequests;
    private OutputEventChannel createDeliveryRequestsApproved;
    private OutputEventChannel createDeliveryRequestsRejected;
    private OutputEventChannel newDeliveryCreated;

    private InputEventChannel startDeliveryRequests;
    private OutputEventChannel startDeliveryRequestsApproved;
    private OutputEventChannel startDeliveryRequestsRejected;

    private InputEventChannel completeDeliveryRequests;
    private OutputEventChannel completeDeliveryRequestsApproved;
    private OutputEventChannel completeDeliveryRequestsRejected;

    private InputEventChannel getDeliveryRequests;
    private OutputEventChannel getDeliveryRequestsApproved;
    private OutputEventChannel getDeliveryRequestsRejected;

    private InputEventChannel dronePositionReports;

    private InputEventChannel droneAssigned;
    private InputEventChannel droneUnavailable;
    private OutputEventChannel deliveryCompleted;

    public DeliveryServiceEventBasedController(DeliveryService deliveryService, String evChannelsLocation) {
        this.deliveryService = deliveryService;
        this.evChannelsLocation = evChannelsLocation;
    }

    @Override
    public void start() {
        logger.log(Level.INFO, "Delivery Service event channels initializing...");

        createDeliveryRequests = input(CREATE_DELIVERY_REQUESTS_EVC);
        createDeliveryRequestsApproved = output(CREATE_DELIVERY_REQUESTS_APPROVED_EVC);
        createDeliveryRequestsRejected = output(CREATE_DELIVERY_REQUESTS_REJECTED_EVC);
        newDeliveryCreated = output(NEW_DELIVERY_CREATED_EVC);

        startDeliveryRequests = input(START_DELIVERY_REQUESTS_EVC);
        startDeliveryRequestsApproved = output(START_DELIVERY_REQUESTS_APPROVED_EVC);
        startDeliveryRequestsRejected = output(START_DELIVERY_REQUESTS_REJECTED_EVC);

        completeDeliveryRequests = input(COMPLETE_DELIVERY_REQUESTS_EVC);
        completeDeliveryRequestsApproved = output(COMPLETE_DELIVERY_REQUESTS_APPROVED_EVC);
        completeDeliveryRequestsRejected = output(COMPLETE_DELIVERY_REQUESTS_REJECTED_EVC);

        getDeliveryRequests = input(GET_DELIVERY_REQUESTS_EVC);
        getDeliveryRequestsApproved = output(GET_DELIVERY_REQUESTS_APPROVED_EVC);
        getDeliveryRequestsRejected = output(GET_DELIVERY_REQUESTS_REJECTED_EVC);

        dronePositionReports = input(DRONE_POSITION_REPORTS_EVC);

        droneAssigned = input(DRONE_ASSIGNED_EVC);
        droneUnavailable = input(DRONE_UNAVAILABLE_EVC);
        deliveryCompleted = output(DELIVERY_COMPLETED_EVC);

        Future.all(List.of(
                createDeliveryRequests.init(this::scheduleDelivery),
                startDeliveryRequests.init(this::startDelivery),
                completeDeliveryRequests.init(this::completeDelivery),
                getDeliveryRequests.init(this::getDelivery),
                dronePositionReports.init(this::updateDronePosition),
                droneAssigned.init(this::assignDrone),
                droneUnavailable.init(this::noDroneAvailable)))
            .onSuccess(v -> logger.log(Level.INFO, "Delivery Service event channels ready"))
            .onFailure(err -> logger.log(Level.SEVERE, "Event channels unavailable - " + err.getMessage()));
    }

    private InputEventChannel input(String channel) {
        return new InputEventChannel(vertx, channel, evChannelsLocation, CONSUMER_GROUP);
    }

    private OutputEventChannel output(String channel) {
        return new OutputEventChannel(vertx, channel, evChannelsLocation);
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

            approve(createDeliveryRequestsApproved, requestId, delivery);
            announce(delivery);
        } catch (Exception e) {
            reject(createDeliveryRequestsRejected, requestId, e.getMessage());
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

    private void startDelivery(JsonObject request) {
        var requestId = request.getString("requestId");
        logger.log(Level.INFO, "StartDelivery request - " + requestId);
        try {
            var delivery = deliveryService.startDelivery(new DeliveryId(required(request, "deliveryId")));
            approve(startDeliveryRequestsApproved, requestId, delivery);
        } catch (Exception e) {
            reject(startDeliveryRequestsRejected, requestId, e.getMessage());
        }
    }

    private void completeDelivery(JsonObject request) {
        var requestId = request.getString("requestId");
        logger.log(Level.INFO, "CompleteDelivery request - " + requestId);
        try {
            var delivery = deliveryService.completeDelivery(new DeliveryId(required(request, "deliveryId")));
            approve(completeDeliveryRequestsApproved, requestId, delivery);
            deliveryCompleted.postEvent(delivery.getId().value(), new JsonObject()
                .put("deliveryId", delivery.getId().value())
                .put("orderId", delivery.orderId())
                .put("droneId", delivery.droneId()));
        } catch (Exception e) {
            reject(completeDeliveryRequestsRejected, requestId, e.getMessage());
        }
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

    /**
     * Announces the delivery to whoever is subscribed. The payload carries weight and pickup
     * point, which the replies do not: a consumer must be able to act without asking back.
     */
    private void announce(Delivery delivery) {
        var fact = new JsonObject()
            .put("deliveryId", delivery.getId().value())
            .put("orderId", delivery.orderId())
            .put("pickupLat", delivery.route().pickupLat())
            .put("pickupLng", delivery.route().pickupLng())
            .put("weightKg", delivery.weightKg());
        newDeliveryCreated.postEvent(delivery.getId().value(), fact);
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
