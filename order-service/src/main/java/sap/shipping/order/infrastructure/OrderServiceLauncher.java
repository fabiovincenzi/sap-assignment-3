package sap.shipping.order.infrastructure;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import sap.shipping.order.application.OrderService;
import sap.shipping.order.application.UserService;

public class OrderServiceLauncher extends AbstractVerticle {

    static Logger logger = Logger.getLogger("[Order Service]");

    private static final int PORT = 8090;
    private static final int METRICS_PORT = 9490;

    // Externalized Configuration: the default runs on the host, compose and Kubernetes set broker:9092
    private static final String EV_CHANNELS_LOCATION = env("EV_CHANNELS_LOCATION", "localhost:29092");

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value != null ? value : defaultValue;
    }

    @Override
    public void start() {
        logger.log(Level.INFO, "Order Service initializing...");

        var orderService = buildOrderService();
        var userService = new UserService(new InMemoryUserRepository());

        startMetrics(orderService);
        announceOnChannels(orderService);
        startHttpEndpoints(orderService, userService);
    }

    private OrderService buildOrderService() {
        return new OrderService(new InMemoryOrderRepository());
    }

    private void startMetrics(OrderService service) {
        try {
            service.addObserver(new PrometheusOrderServiceObserver(METRICS_PORT));
            logger.log(Level.INFO, "Prometheus metrics server ready - port: " + METRICS_PORT);
        } catch (ObsMetricServerException e) {
            logger.log(Level.SEVERE, "Failed to start Prometheus metrics server - " + e.getMessage());
        }
    }

    // the announcement leaves through the same mechanism as the metrics: an observer
    private void announceOnChannels(OrderService service) {
        service.addObserver(new KafkaOrderServiceObserver(vertx, EV_CHANNELS_LOCATION));
    }

    private void startHttpEndpoints(OrderService orderService, UserService userService) {
        vertx.createHttpServer()
            .requestHandler(new OrderController(orderService, userService).createRouter(vertx))
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
