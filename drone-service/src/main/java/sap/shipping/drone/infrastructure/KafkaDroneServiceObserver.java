package sap.shipping.drone.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import sap.shipping.common.exagonal.Adapter;
import sap.shipping.common.kafka.OutputEventChannel;
import sap.shipping.drone.application.DroneServiceObserver;

import java.util.logging.Level;
import java.util.logging.Logger;

/** Reports where a drone is. A report expects no answer, so there is no reply channel. */
@Adapter
public class KafkaDroneServiceObserver implements DroneServiceObserver {

    static Logger logger = Logger.getLogger("[Drone Kafka Observer]");

    static final String DRONE_POSITION_REPORTS_EVC = "drone-position-reports";

    private final OutputEventChannel dronePositionReports;

    public KafkaDroneServiceObserver(Vertx vertx, String evChannelsLocation) {
        this.dronePositionReports =
            new OutputEventChannel(vertx, DRONE_POSITION_REPORTS_EVC, evChannelsLocation);
    }

    @Override
    public void notifyDroneAvailable(String droneId) {
        // the fleet is the drone service's own business: nobody outside acts on it
    }

    @Override
    public void notifyDroneAssigned(String droneId) {
        // announced by the assignment flow instead, which carries the delivery it belongs to
    }

    @Override
    public void notifyLocationUpdated(String droneId, double lat, double lng) {
        // keyed by drone: the positions of one drone must stay in the order they were reported
        dronePositionReports.postEvent(droneId, new JsonObject()
                .put("droneId", droneId)
                .put("lat", lat)
                .put("lng", lng))
            .onFailure(err -> logger.log(Level.WARNING,
                "position of drone " + droneId + " not reported - " + err.getMessage()));
    }
}
