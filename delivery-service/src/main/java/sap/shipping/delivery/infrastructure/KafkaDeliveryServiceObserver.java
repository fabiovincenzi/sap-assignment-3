package sap.shipping.delivery.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import sap.shipping.common.exagonal.Adapter;
import sap.shipping.common.kafka.OutputEventChannel;
import sap.shipping.delivery.application.DeliveryServiceObserver;
import sap.shipping.delivery.domain.events.DeliveryCompleted;
import sap.shipping.delivery.domain.events.DeliveryScheduled;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Announces the facts the delivery domain produces on its own, so that the announcement does not
 * depend on which adapter handled the request. The payload carries everything a consumer needs to
 * act without asking back.
 *
 * The replies that travel with a requestId are not announced here, because the domain knows
 * nothing about who asked: those stay in the controller that received the request.
 */
@Adapter
public class KafkaDeliveryServiceObserver implements DeliveryServiceObserver {

    static Logger logger = Logger.getLogger("[Delivery Kafka Observer]");

    static final String NEW_DELIVERY_CREATED_EVC = "new-delivery-created";

    private final OutputEventChannel newDeliveryCreated;

    public KafkaDeliveryServiceObserver(Vertx vertx, String evChannelsLocation) {
        this.newDeliveryCreated = new OutputEventChannel(vertx, NEW_DELIVERY_CREATED_EVC, evChannelsLocation);
    }

    @Override
    public void notifyDeliveryScheduled(DeliveryScheduled event) {
        var deliveryId = event.deliveryId().value();
        /* weight and pickup point travel with the fact: whoever owns the fleet must be able to
           pick a drone without querying this service back */
        newDeliveryCreated.postEvent(deliveryId, new JsonObject()
                .put("deliveryId", deliveryId)
                .put("orderId", event.orderId())
                .put("pickupLat", event.route().pickupLat())
                .put("pickupLng", event.route().pickupLng())
                .put("weightKg", event.weightKg()))
            .onFailure(err -> logger.log(Level.SEVERE,
                "delivery " + deliveryId + " scheduled but not announced - " + err.getMessage()));
    }

    @Override
    public void notifyDeliveryCompleted(DeliveryCompleted event) {
        /* not announced: no service consumes the completion today, and the flow the gateway
           exposes stops at DRONE_ASSIGNED. A channel without a consumer would be dead weight */
    }
}
