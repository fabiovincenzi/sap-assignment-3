package sap.shipping.drone.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import sap.shipping.common.exagonal.Adapter;
import sap.shipping.common.kafka.OutputEventChannel;
import sap.shipping.drone.application.DeliveryServicePort;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reports where a drone is, on the channel the delivery service listens to. A report expects no
 * answer, so there is no reply channel and nothing to wait for.
 */
@Adapter
public class DeliveryServiceEventBasedProxy implements DeliveryServicePort {

    static Logger logger = Logger.getLogger("[Drone DeliveryEventProxy]");

    static final String DRONE_POSITION_REPORTS_EVC = "drone-position-reports";

    private final OutputEventChannel dronePositionReports;

    public DeliveryServiceEventBasedProxy(Vertx vertx, String evChannelsLocation) {
        this.dronePositionReports =
            new OutputEventChannel(vertx, DRONE_POSITION_REPORTS_EVC, evChannelsLocation);
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
