package sap.shipping.delivery.application;

import sap.shipping.delivery.domain.*;
import sap.shipping.delivery.domain.events.DeliveryCompleted;
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
        repository.save(delivery);
        logger.log(Level.INFO, "schedule delivery " + delivery.getId().value() + " for order " + orderId);
        observers.forEach(o -> o.notifyDeliveryScheduled(delivery.getId().value()));
        /* the drone is not requested here: saving announces the delivery, and whoever owns the
           fleet answers with a drone of its own accord */
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
        repository.save(delivery);
        logger.log(Level.INFO, "complete delivery " + deliveryId.value());
        observers.forEach(o -> o.notifyDeliveryCompleted(deliveryId.value()));
        /* neither the order nor the fleet is called: saving announces the completion, and each
           of them decides what to do with it */
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
