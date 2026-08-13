package sap.shipping.order.infrastructure;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import sap.shipping.common.exagonal.Adapter;
import sap.shipping.common.kafka.InputEventChannel;
import sap.shipping.order.application.OrderService;
import sap.shipping.order.domain.OrderId;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Lets the order service learn that one of its orders has been delivered, by listening instead
 * of being told. The order service keeps its REST architecture: this is the one adaptation
 * needed to interact with the redesigned delivery service.
 */
@Adapter
public class OrderServiceEventBasedController extends AbstractVerticle {

    static Logger logger = Logger.getLogger("[Order Service Event-Based Controller]");

    static final String DELIVERY_COMPLETED_EVC = "delivery-completed";
    static final String CONSUMER_GROUP = "order-service";

    private final OrderService orderService;
    private final String evChannelsLocation;

    private InputEventChannel deliveryCompleted;

    public OrderServiceEventBasedController(OrderService orderService, String evChannelsLocation) {
        this.orderService = orderService;
        this.evChannelsLocation = evChannelsLocation;
    }

    @Override
    public void start() {
        deliveryCompleted =
            new InputEventChannel(vertx, DELIVERY_COMPLETED_EVC, evChannelsLocation, CONSUMER_GROUP);
        deliveryCompleted.init(this::completeOrder)
            .onSuccess(v -> logger.log(Level.INFO, "Order Service event channels ready"))
            .onFailure(err -> logger.log(Level.SEVERE, "Event channels unavailable - " + err.getMessage()));
    }

    private void completeOrder(JsonObject fact) {
        var orderId = fact.getString("orderId");
        logger.log(Level.INFO, "DeliveryCompleted - completing order " + orderId);
        try {
            orderService.completeOrder(new OrderId(orderId));
        } catch (Exception e) {
            logger.log(Level.WARNING, "Order " + orderId + " not completed - " + e.getMessage());
        }
    }
}
