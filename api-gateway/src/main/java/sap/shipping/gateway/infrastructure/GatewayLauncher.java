package sap.shipping.gateway.infrastructure;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;

/**
 * Composition root of the API Gateway: wires the three proxies into the
 * controller and starts the HTTP server on port 8080.
 */
public class GatewayLauncher extends AbstractVerticle {

    static Logger logger = Logger.getLogger("[API Gateway]");

    private static final int PORT = 8080;
    private static final int METRICS_PORT = 9492;

    /* Externalized Configuration: the defaults run on the host, compose and Kubernetes override
       them with the service names. The delivery service is reached over channels, not over HTTP. */
    private static final String ORDER_HOST = env("ORDER_HOST", "localhost");
    private static final int ORDER_PORT = Integer.parseInt(env("ORDER_PORT", "8090"));
    private static final String DRONE_HOST = env("DRONE_HOST", "localhost");
    private static final int DRONE_PORT = Integer.parseInt(env("DRONE_PORT", "8092"));
    private static final String EV_CHANNELS_LOCATION = env("EV_CHANNELS_LOCATION", "localhost:29092");

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value != null ? value : defaultValue;
    }

    @Override
    public void start() {
        logger.log(Level.INFO, "API Gateway initializing...");

        var deliveryProxy = startDeliveryChannels();
        var controller = buildController(deliveryProxy);

        startHttpEndpoints(controller);
    }

    /* the only proxy that has to be started: it subscribes to the reply channels, and until
       that completes no request can be answered */
    private DeliveryServiceEventBasedProxy startDeliveryChannels() {
        var proxy = new DeliveryServiceEventBasedProxy(vertx, EV_CHANNELS_LOCATION);
        proxy.start()
            .onSuccess(v -> logger.log(Level.INFO, "Delivery event channels ready"))
            .onFailure(err -> logger.log(Level.SEVERE,
                "Delivery event channels unavailable - " + err.getMessage()));
        return proxy;
    }

    // order and drone keep their REST architecture: only the delivery service was redesigned
    private GatewayController buildController(DeliveryServiceEventBasedProxy deliveryProxy) {
        var orderProxy = new OrderServiceProxy(ORDER_HOST, ORDER_PORT);
        var droneProxy = new DroneServiceProxy(DRONE_HOST, DRONE_PORT);
        return new GatewayController(orderProxy, deliveryProxy, droneProxy, buildMetrics());
    }

    /* null when the metrics server could not start: the gateway serves requests anyway,
       and the controller skips the measurement */
    private GatewayMetrics buildMetrics() {
        try {
            var metrics = new GatewayMetrics(METRICS_PORT);
            logger.log(Level.INFO, "Prometheus metrics server ready - port: " + METRICS_PORT);
            return metrics;
        } catch (ObsMetricServerException e) {
            logger.log(Level.SEVERE, "Failed to start Prometheus metrics server - " + e.getMessage());
            return null;
        }
    }

    private void startHttpEndpoints(GatewayController controller) {
        vertx.createHttpServer()
            .requestHandler(controller.createRouter(vertx))
            .listen(PORT)
            .onSuccess(s -> logger.log(Level.INFO, "API Gateway ready - port: " + PORT))
            .onFailure(err -> {
                logger.log(Level.SEVERE, "API Gateway failed to bind port " + PORT + " - " + err.getMessage());
                vertx.close().onComplete(v -> System.exit(1));
            });
    }

    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(new GatewayLauncher());
    }
}
