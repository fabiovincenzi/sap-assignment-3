package sap.shipping.order.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import sap.shipping.common.exagonal.Adapter;
import sap.shipping.common.kafka.OutputEventChannel;
import sap.shipping.order.application.OrderServiceObserver;
import sap.shipping.order.domain.events.OrderConfirmed;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Announces that an order has been confirmed. The payload carries the whole shipment, because a
 * consumer must be able to act on the fact without asking anything back.
 */
@Adapter
public class KafkaOrderServiceObserver implements OrderServiceObserver {

    static Logger logger = Logger.getLogger("[Order Kafka Observer]");

    static final String ORDER_CONFIRMED_EVC = "order-confirmed";

    private final OutputEventChannel orderConfirmed;

    public KafkaOrderServiceObserver(Vertx vertx, String evChannelsLocation) {
        this.orderConfirmed = new OutputEventChannel(vertx, ORDER_CONFIRMED_EVC, evChannelsLocation);
    }

    @Override
    public void notifyOrderCreated(String orderId) {
        // an order that exists but is not confirmed concerns nobody outside this service
    }

    @Override
    public void notifyOrderConfirmed(OrderConfirmed event) {
        var orderId = event.orderId().value();
        orderConfirmed.postEvent(orderId, new JsonObject()
                .put("orderId", orderId)
                .put("pickupLat", event.pickup().lat())
                .put("pickupLng", event.pickup().lng())
                .put("deliveryLat", event.delivery().lat())
                .put("deliveryLng", event.delivery().lng())
                .put("weightKg", event.packageInfo().weightKg()))
            .onFailure(err -> logger.log(Level.SEVERE,
                "order " + orderId + " confirmed but not announced - " + err.getMessage()));
    }
}
