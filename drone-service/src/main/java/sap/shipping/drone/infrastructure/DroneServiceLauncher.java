package sap.shipping.drone.infrastructure;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import sap.shipping.drone.application.DroneService;

public class DroneServiceLauncher extends AbstractVerticle {

    static Logger logger = Logger.getLogger("[Drone Service]");

    private static final int PORT = 8092;
    private static final int METRICS_PORT = 9493;

    // Externalized Configuration: the default runs on the host, compose and Kubernetes set broker:9092
    private static final String EV_CHANNELS_LOCATION = env("EV_CHANNELS_LOCATION", "localhost:29092");

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value != null ? value : defaultValue;
    }

    @Override
    public void start() {
        logger.log(Level.INFO, "Drone Service initializing...");

        var service = buildService();
        startMetrics(service);
        reportOnChannels(service);
        startEventChannels(service);
        startHttpEndpoints(service);
    }

    private DroneService buildService() {
        return new DroneService(new InMemoryDroneRepository());
    }

    private void startMetrics(DroneService service) {
        try {
            service.addObserver(new PrometheusDroneServiceObserver(METRICS_PORT));
            logger.log(Level.INFO, "Prometheus metrics server ready - port: " + METRICS_PORT);
        } catch (ObsMetricServerException e) {
            logger.log(Level.SEVERE, "Failed to start Prometheus metrics server - " + e.getMessage());
        }
    }

    // the report leaves through the same mechanism as the metrics: an observer
    private void reportOnChannels(DroneService service) {
        service.addObserver(new KafkaDroneServiceObserver(vertx, EV_CHANNELS_LOCATION));
    }

    private void startEventChannels(DroneService service) {
        vertx.deployVerticle(new DroneServiceEventBasedController(service, EV_CHANNELS_LOCATION));
    }

    // the drone service keeps its REST API: only the delivery service was redesigned
    private void startHttpEndpoints(DroneService service) {
        vertx.createHttpServer()
            .requestHandler(new DroneController(service).createRouter(vertx))
            .listen(PORT)
            .onSuccess(s -> logger.log(Level.INFO, "Drone Service ready - port: " + PORT))
            .onFailure(err -> {
                logger.log(Level.SEVERE, "Drone Service failed to bind port " + PORT + " - " + err.getMessage());
                vertx.close().onComplete(v -> System.exit(1));
            });
    }

    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(new DroneServiceLauncher());
    }
}
