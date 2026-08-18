package sap.shipping.drone.application;

import sap.shipping.common.exagonal.OutBoundPort;

/**
 * Everything the drone service lets out passes from here: metrics and reports alike. It names no
 * recipient, so the service does not depend on who is listening.
 */
@OutBoundPort
public interface DroneServiceObserver {

    void notifyDroneAvailable(String droneId);

    void notifyDroneAssigned(String droneId);

    void notifyLocationUpdated(String droneId, double lat, double lng);
}
