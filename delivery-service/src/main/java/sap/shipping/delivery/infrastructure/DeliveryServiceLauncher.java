package sap.shipping.delivery.infrastructure;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import sap.shipping.delivery.application.DeliveryService;
import sap.shipping.delivery.application.DeliveryServiceImpl;

public class DeliveryServiceLauncher extends AbstractVerticle {

    static Logger logger = Logger.getLogger("[Delivery Service]");

    // Only /health is served here: the domain is reached through the event channels.
    private static final int PORT = 8091;
    private static final int METRICS_PORT = 9491;

    // Externalized Configuration: the default runs on the host, compose and Kubernetes set broker:9092
    private static final String EV_CHANNELS_LOCATION = env("EV_CHANNELS_LOCATION", "localhost:29092");

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value != null ? value : defaultValue;
    }

    @Override
    public void start() {
        logger.log(Level.INFO, "Delivery Service initializing...");

        var service = buildService();
        startMetrics(service);
        startEventChannels(service);
        startHealthEndpoint();
    }

    // the only place where the concrete adapters meet the core, which declares just its ports
    private DeliveryService buildService() {
        var eventStore = new InMemoryDeliveryEventStore();
        var snapshotStore = new InMemoryDeliverySnapshotStore();
        var lookupView = new InMemoryDeliveryLookupView();

        var repo = new EventSourcedDeliveryRepository(eventStore, snapshotStore, lookupView);
        return new DeliveryServiceImpl(repo);
    }

    private void startMetrics(DeliveryService service) {
        try {
            service.addObserver(new PrometheusDeliveryServiceObserver(METRICS_PORT));
            logger.log(Level.INFO, "Prometheus metrics server ready - port: " + METRICS_PORT);
        } catch (ObsMetricServerException e) {
            logger.log(Level.SEVERE, "Failed to start Prometheus metrics server - " + e.getMessage());
        }
    }

    private void startEventChannels(DeliveryService service) {
        vertx.deployVerticle(new DeliveryServiceEventBasedController(service, EV_CHANNELS_LOCATION));
    }

    private void startHealthEndpoint() {
        vertx.createHttpServer()
            .requestHandler(new DeliveryController().createRouter(vertx))
            .listen(PORT)
            .onSuccess(s -> logger.log(Level.INFO, "Delivery Service ready - port: " + PORT))
            .onFailure(err -> {
                logger.log(Level.SEVERE, "Delivery Service failed to bind port " + PORT + " - " + err.getMessage());
                vertx.close().onComplete(v -> System.exit(1));
            });
    }

    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(new DeliveryServiceLauncher());
    }
}
