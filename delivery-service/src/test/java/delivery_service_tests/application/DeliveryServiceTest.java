package delivery_service_tests.application;

import org.junit.jupiter.api.Test;
import sap.shipping.delivery.application.*;
import sap.shipping.delivery.domain.*;
import sap.shipping.delivery.infrastructure.*;
import static org.assertj.core.api.Assertions.*;

public class DeliveryServiceTest {

    /**
     * The service no longer calls anyone: the drone arrives as an answer to the announced
     * delivery, so it is handed in from outside instead of being fetched.
     */
    @Test
    public void aDeliveryIsBornWithoutADroneAndReachesItThroughAssignment() {
        var repo = new EventSourcedDeliveryRepository(new InMemoryDeliveryEventStore(),
            new InMemoryDeliverySnapshotStore(), new InMemoryDeliveryLookupView());
        var service = new DeliveryServiceImpl(repo);

        var scheduled = service.scheduleDelivery("order-9", 44.0, 12.0, 44.1, 12.1, 2.0);
        assertThat(scheduled.status()).isEqualTo(DeliveryStatus.SCHEDULED);
        assertThat(scheduled.droneId()).isNull();

        var assigned = service.assignDrone(scheduled.getId(), "drone-1");
        assertThat(assigned.status()).isEqualTo(DeliveryStatus.DRONE_ASSIGNED);
        assertThat(assigned.droneId()).isEqualTo("drone-1");

        service.startDelivery(scheduled.getId());
        var completed = service.completeDelivery(scheduled.getId());
        assertThat(completed.status()).isEqualTo(DeliveryStatus.DELIVERED);
    }
}
