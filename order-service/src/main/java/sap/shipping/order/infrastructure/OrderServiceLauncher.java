package sap.shipping.order.infrastructure;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import sap.shipping.order.application.OrderService;
import sap.shipping.order.application.UserService;
;

public class OrderServiceLauncher extends AbstractVerticle {

    static Logger logger = Logger.getLogger("[Order Service]");

    private static final int PORT = 8090;
    private static final int METRICS_PORT = 9490;

    /* Externalized configuration: defaults target a manual local deployment,
       the docker-compose file overrides them with the service container names. */
    /* the host listener of the broker, overridden with the internal one inside compose */
    private static final String EV_CHANNELS_LOCATION = env("EV_CHANNELS_LOCATION", "localhost:29092");
    private static final String DELIVERY_HOST = env("DELIVERY_HOST", "localhost");
    private static final int DELIVERY_PORT = Integer.parseInt(env("DELIVERY_PORT", "8091"));

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value != null ? value : defaultValue;
    }

    @Override
    public void start() {
        logger.log(Level.INFO, "Order Service initializing...");
        var orderRepo = new InMemoryOrderRepository();
        var userRepo = new InMemoryUserRepository();
        var deliveryProxy = new DeliveryServiceEventBasedProxy(vertx, EV_CHANNELS_LOCATION);

        var orderService = new OrderService(orderRepo, deliveryProxy);
        vertx.deployVerticle(new OrderServiceEventBasedController(orderService, EV_CHANNELS_LOCATION));
        var userService = new UserService(userRepo);
        var controller = new OrderController(orderService, userService);

        try {
            orderService.addObserver(new PrometheusOrderServiceObserver(METRICS_PORT));
            logger.log(Level.INFO, "Prometheus metrics server ready - port: " + METRICS_PORT);
        } catch (ObsMetricServerException e) {
            logger.log(Level.SEVERE, "Failed to start Prometheus metrics server - " + e.getMessage());
        }

        vertx.createHttpServer()
            .requestHandler(controller.createRouter(vertx))
            .listen(PORT)
            .onSuccess(s -> logger.log(Level.INFO, "Order Service ready - port: " + PORT))
            .onFailure(err -> {
                logger.log(Level.SEVERE, "Order Service failed to bind port " + PORT + " - " + err.getMessage());
                vertx.close().onComplete(v -> System.exit(1));
            });
    }

    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(new OrderServiceLauncher());
    }
}
