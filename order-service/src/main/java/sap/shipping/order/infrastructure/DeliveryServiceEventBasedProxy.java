package sap.shipping.order.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import sap.shipping.common.exagonal.Adapter;
import sap.shipping.common.kafka.OutputEventChannel;
import sap.shipping.order.application.DeliveryServicePort;
import sap.shipping.order.domain.events.OrderConfirmed;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Asks the delivery service for a delivery through an event channel. The reply channels are
 * not subscribed on purpose: confirming an order does not wait for the delivery to exist.
 */
@Adapter
public class DeliveryServiceEventBasedProxy implements DeliveryServicePort {

    static Logger logger = Logger.getLogger("[Order DeliveryEventProxy]");

    static final String CREATE_DELIVERY_REQUESTS_EVC = "create-delivery-requests";

    private final OutputEventChannel createDeliveryRequests;

    public DeliveryServiceEventBasedProxy(Vertx vertx, String evChannelsLocation) {
        this.createDeliveryRequests =
            new OutputEventChannel(vertx, CREATE_DELIVERY_REQUESTS_EVC, evChannelsLocation);
    }

    @Override
    public void notifyOrderConfirmed(OrderConfirmed event) {
        var requestId = UUID.randomUUID().toString();
        var request = new JsonObject()
            .put("requestId", requestId)
            .put("orderId", event.orderId().value())
            .put("pickupLat", event.pickup().lat())
            .put("pickupLng", event.pickup().lng())
            .put("deliveryLat", event.delivery().lat())
            .put("deliveryLng", event.delivery().lng())
            .put("weightKg", event.packageInfo().weightKg());
        createDeliveryRequests.postEvent(requestId, request)
            .onSuccess(v -> logger.log(Level.INFO,
                "delivery requested for order " + event.orderId().value()))
            .onFailure(err -> logger.log(Level.SEVERE,
                "delivery request not posted for order " + event.orderId().value()
                    + " - " + err.getMessage()));
    }
}
