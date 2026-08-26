package sap.shipping.delivery.application;

import sap.shipping.delivery.domain.*;
import sap.shipping.delivery.domain.events.DeliveryCompleted;
import sap.shipping.delivery.domain.events.DeliveryScheduled;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeliveryServiceImpl implements DeliveryService {

    static Logger logger = Logger.getLogger("[Delivery Service]");

    private final DeliveryRepository repository;
    private final List<DeliveryServiceObserver> observers = new ArrayList<>();

    public DeliveryServiceImpl(DeliveryRepository repository) {
        this.repository = repository;
    }

    @Override
    public void addObserver(DeliveryServiceObserver observer) {
        observers.add(observer);
    }

    @Override
    public Delivery scheduleDelivery(String orderId, double pickupLat, double pickupLng,
                                      double deliveryLat, double deliveryLng, double weightKg) {
        var route = new Route(pickupLat, pickupLng, deliveryLat, deliveryLng);
        var delivery = new Delivery(DeliveryId.generate(), orderId, route, weightKg);
        /* read the events before saving: the repository hands them to the event store and clears them */
        var events = List.copyOf(delivery.pendingEvents());
        repository.save(delivery);
        logger.log(Level.INFO, "schedule delivery " + delivery.getId().value() + " for order " + orderId);
        /* the drone is not requested here: the observers announce the delivery, and whoever owns
           the fleet answers with a drone of its own accord */
        events.stream()
            .filter(e -> e instanceof DeliveryScheduled)
            .map(e -> (DeliveryScheduled) e)
            .forEach(e -> observers.forEach(o -> o.notifyDeliveryScheduled(e)));
        return delivery;
    }

    @Override
    public Delivery assignDrone(DeliveryId deliveryId, String droneId) {
        var delivery = repository.findById(deliveryId)
            .orElseThrow(() -> new IllegalArgumentException("Delivery not found: " + deliveryId));
        delivery.assignDrone(droneId);
        repository.save(delivery);
        logger.log(Level.INFO, "drone " + droneId + " assigned to delivery " + deliveryId.value());
        return delivery;
    }

    @Override
    public Delivery startDelivery(DeliveryId deliveryId) {
        var delivery = repository.findById(deliveryId)
            .orElseThrow(() -> new IllegalArgumentException("Delivery not found: " + deliveryId));
        delivery.startTransit();
        repository.save(delivery);
        logger.log(Level.INFO, "start delivery " + deliveryId.value());
        return delivery;
    }

    @Override
    public void updateDronePosition(String droneId, double lat, double lng) {
        var delivery = repository.findByDroneId(droneId);
        if (delivery.isPresent()) {
            delivery.get().updatePosition(lat, lng);
            repository.save(delivery.get());
            logger.log(Level.INFO, "update position of delivery " + delivery.get().getId().value()
                + " to (" + lat + ", " + lng + ")");
        } else {
            logger.log(Level.WARNING, "no delivery found for drone " + droneId);
        }
    }

    @Override
    public Delivery completeDelivery(DeliveryId deliveryId) {
        var delivery = repository.findById(deliveryId)
            .orElseThrow(() -> new IllegalArgumentException("Delivery not found: " + deliveryId));
        delivery.complete();
        /* read the events before saving: the repository hands them to the event store and clears them */
        var events = List.copyOf(delivery.pendingEvents());
        repository.save(delivery);
        logger.log(Level.INFO, "complete delivery " + deliveryId.value());
        /* the completion reaches the observers, but no service listens to it in this assignment:
           the flow the gateway exposes stops at DRONE_ASSIGNED, so only the metrics react */
        events.stream()
            .filter(e -> e instanceof DeliveryCompleted)
            .map(e -> (DeliveryCompleted) e)
            .forEach(e -> observers.forEach(o -> o.notifyDeliveryCompleted(e)));
        return delivery;
    }

    @Override
    public Optional<Delivery> trackDelivery(DeliveryId deliveryId) {
        return repository.findById(deliveryId);
    }

    @Override
    public Optional<Delivery> findByOrderId(String orderId) {
        return repository.findByOrderId(orderId);
    }
}
