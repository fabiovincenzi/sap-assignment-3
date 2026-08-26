package sap.shipping.delivery.application;

import sap.shipping.common.exagonal.OutBoundPort;
import sap.shipping.delivery.domain.events.DeliveryCompleted;
import sap.shipping.delivery.domain.events.DeliveryScheduled;

/**
 * Everything the delivery service lets out passes from here: metrics and announcements alike. It
 * names no recipient, so the service does not depend on who is listening.
 *
 * The domain events are handed over whole: an observer that announces the fact on the bus needs
 * more than an id, and one that only counts is free to ignore the rest.
 */
@OutBoundPort
public interface DeliveryServiceObserver {

    void notifyDeliveryScheduled(DeliveryScheduled event);

    void notifyDeliveryCompleted(DeliveryCompleted event);
}
