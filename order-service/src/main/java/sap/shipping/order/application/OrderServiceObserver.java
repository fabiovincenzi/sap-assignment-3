package sap.shipping.order.application;

import sap.shipping.common.exagonal.OutBoundPort;
import sap.shipping.order.domain.events.OrderConfirmed;

/**
 * Everything the order service lets out passes from here: metrics and announcements alike. It
 * names no recipient, so the service does not depend on who is listening.
 */
@OutBoundPort
public interface OrderServiceObserver {

    void notifyOrderCreated(String orderId);

    void notifyOrderConfirmed(OrderConfirmed event);
}
