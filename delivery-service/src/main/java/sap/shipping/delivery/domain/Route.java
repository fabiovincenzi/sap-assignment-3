package sap.shipping.delivery.domain;

import sap.shipping.common.ddd.ValueObject;
import java.util.Objects;

public class Route implements ValueObject {

    private static final int EARTH_RADIUS_KM = 6371;

    private final double pickupLat;
    private final double pickupLng;
    private final double deliveryLat;
    private final double deliveryLng;
    private final double distanceKm;

    public Route(double pickupLat, double pickupLng, double deliveryLat, double deliveryLng) {
        this.pickupLat = pickupLat;
        this.pickupLng = pickupLng;
        this.deliveryLat = deliveryLat;
        this.deliveryLng = deliveryLng;
        this.distanceKm = calculateDistance(pickupLat, pickupLng, deliveryLat, deliveryLng);
    }

    public double pickupLat() { return pickupLat; }
    public double pickupLng() { return pickupLng; }
    public double deliveryLat() { return deliveryLat; }
    public double deliveryLng() { return deliveryLng; }
    public double distanceKm() { return distanceKm; }

    // 2 min for 1 km
    public long estimatedMinutes() {
        return Math.max(1, Math.round(distanceKm / 0.5));
    }

    // Straight-line distance in km.
    private static double calculateDistance(double lat1, double lng1, double lat2, double lng2) {
        double x = Math.toRadians(lng2 - lng1) * Math.cos(Math.toRadians((lat1 + lat2) / 2));
        double y = Math.toRadians(lat2 - lat1);
        return EARTH_RADIUS_KM * Math.sqrt(x * x + y * y);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Route r = (Route) o;
        return Double.compare(pickupLat, r.pickupLat) == 0 && Double.compare(pickupLng, r.pickupLng) == 0
            && Double.compare(deliveryLat, r.deliveryLat) == 0 && Double.compare(deliveryLng, r.deliveryLng) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pickupLat, pickupLng, deliveryLat, deliveryLng);
    }
}
