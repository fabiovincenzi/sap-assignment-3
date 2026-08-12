package sap.shipping.gateway.infrastructure;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import sap.shipping.common.exagonal.Adapter;
import sap.shipping.common.kafka.InputEventChannel;
import sap.shipping.common.kafka.OutputEventChannel;
import sap.shipping.gateway.application.DeliveryServicePort;
import sap.shipping.gateway.application.ServiceNotAvailableException;
import sap.shipping.gateway.domain.DeliveryView;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reaches the delivery service through event channels. Replies of every caller share the same
 * channel, so each request carries an id and the proxy keeps a pending answer per id.
 *
 * Blocking is safe here: the controller invokes the port inside executeBlocking.
 */
@Adapter
public class DeliveryServiceEventBasedProxy implements DeliveryServicePort {

    static Logger logger = Logger.getLogger("[Gateway DeliveryEventProxy]");

    static final String GET_DELIVERY_REQUESTS_EVC = "get-delivery-requests";
    static final String GET_DELIVERY_REQUESTS_APPROVED_EVC = "get-delivery-requests-approved";
    static final String GET_DELIVERY_REQUESTS_REJECTED_EVC = "get-delivery-requests-rejected";

    static final String CONSUMER_GROUP = "api-gateway";

    private static final long REPLY_TIMEOUT_SECONDS = 5;

    private final Map<String, CompletableFuture<JsonObject>> pending = new ConcurrentHashMap<>();

    private final OutputEventChannel getDeliveryRequests;
    private final InputEventChannel approved;
    private final InputEventChannel rejected;

    public DeliveryServiceEventBasedProxy(Vertx vertx, String evChannelsLocation) {
        this.getDeliveryRequests = new OutputEventChannel(vertx, GET_DELIVERY_REQUESTS_EVC, evChannelsLocation);
        this.approved = new InputEventChannel(vertx, GET_DELIVERY_REQUESTS_APPROVED_EVC,
            evChannelsLocation, CONSUMER_GROUP);
        this.rejected = new InputEventChannel(vertx, GET_DELIVERY_REQUESTS_REJECTED_EVC,
            evChannelsLocation, CONSUMER_GROUP);
    }

    /** Subscribes to the reply channels. Until this completes no request can be answered. */
    public Future<Void> start() {
        return Future.all(approved.init(this::complete), rejected.init(this::complete)).mapEmpty();
    }

    /** Hands a reply to whoever is waiting for it, if anyone still is. */
    private void complete(JsonObject reply) {
        var waiting = pending.remove(reply.getString("requestId"));
        if (waiting != null) {
            waiting.complete(reply);
        }
    }

    @Override
    public Optional<DeliveryView> findByOrderId(String orderId) {
        var requestId = UUID.randomUUID().toString();
        var answer = new CompletableFuture<JsonObject>();
        pending.put(requestId, answer);

        getDeliveryRequests.postEvent(requestId,
            new JsonObject().put("requestId", requestId).put("orderId", orderId));

        try {
            var reply = answer.get(REPLY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            /* a rejection is how "no delivery for this order" travels on a channel */
            return reply.containsKey("error") ? Optional.empty() : Optional.of(toView(reply));
        } catch (TimeoutException e) {
            logger.log(Level.WARNING, "no reply for request " + requestId + " within "
                + REPLY_TIMEOUT_SECONDS + "s");
            throw new ServiceNotAvailableException("delivery-service did not answer");
        } catch (Exception e) {
            throw new ServiceNotAvailableException("delivery-service not reachable");
        } finally {
            pending.remove(requestId);
        }
    }

    private DeliveryView toView(JsonObject j) {
        return new DeliveryView(
            j.getString("id"),
            j.getString("orderId"),
            j.getString("status"),
            j.getString("droneId"),
            j.getDouble("currentLat", 0.0),
            j.getDouble("currentLng", 0.0),
            j.getInteger("estimatedMinutes", 0),
            j.getString("createdAt"));
    }
}
