package sap.shipping.delivery.application;

import sap.shipping.common.ddd.DomainEvent;
import sap.shipping.common.exagonal.OutBoundPort;
import sap.shipping.delivery.domain.DeliveryId;

/**
 * Announces the domain events of a delivery to whoever is interested. Derived from the event
 * stream like the read model: dropping it leaves the service correct.
 */
@OutBoundPort
public interface DeliveryEventPublisher {

    void publish(DeliveryId deliveryId, DomainEvent event);

    /** Used where the events are persisted but not announced, such as in tests. */
    DeliveryEventPublisher NO_OP = (deliveryId, event) -> { };
}
