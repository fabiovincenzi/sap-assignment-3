package delivery_service_tests.component_tests.steps;

import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import sap.shipping.common.kafka.InputEventChannel;
import sap.shipping.common.kafka.OutputEventChannel;
import sap.shipping.delivery.application.DeliveryServiceImpl;
import sap.shipping.delivery.infrastructure.DeliveryServiceEventBasedController;
import sap.shipping.delivery.infrastructure.EventSourcedDeliveryRepository;
import sap.shipping.delivery.infrastructure.InMemoryDeliveryEventStore;
import sap.shipping.delivery.infrastructure.InMemoryDeliveryLookupView;
import sap.shipping.delivery.infrastructure.InMemoryDeliverySnapshotStore;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the whole delivery service through its event channels, the way a caller would: the
 * service is started for real, only the broker is shared with the environment.
 */
public class DeliverySteps {

    private static final String CREATE_REQUESTS = "create-delivery-requests";
    private static final String CREATE_APPROVED = "create-delivery-requests-approved";

    private Vertx vertx;
    private OutputEventChannel requests;
    private InputEventChannel approved;
    private final CompletableFuture<JsonObject> reply = new CompletableFuture<>();
    private String requestId;
    private JsonObject currentDelivery;

    @Before
    public void startTheService() throws Exception {
        BrokerAvailability.assumeReachable();
        var broker = BrokerAvailability.address();
        vertx = Vertx.vertx();

        var repository = new EventSourcedDeliveryRepository(new InMemoryDeliveryEventStore(),
            new InMemoryDeliverySnapshotStore(), new InMemoryDeliveryLookupView());
        var service = new DeliveryServiceImpl(repository);

        vertx.deployVerticle(new DeliveryServiceEventBasedController(service, broker))
            .toCompletionStage().toCompletableFuture().get(20, TimeUnit.SECONDS);

        requestId = UUID.randomUUID().toString();
        requests = new OutputEventChannel(vertx, CREATE_REQUESTS, broker);
        approved = new InputEventChannel(vertx, CREATE_APPROVED, broker, "component-test-" + requestId);
        approved.init(event -> {
            if (requestId.equals(event.getString("requestId"))) {
                reply.complete(event);
            }
        }).toCompletionStage().toCompletableFuture().get(20, TimeUnit.SECONDS);
    }

    @After
    public void stopTheService() {
        if (vertx != null) {
            vertx.close();
        }
    }

    @When("a delivery is scheduled for order {string} from \\({double}, {double}) to \\({double}, {double}) weighing {double} kg")
    public void schedule_delivery(String orderId, double pLat, double pLng, double dLat, double dLng, double weight)
            throws Exception {
        requests.postEvent(requestId, new JsonObject()
            .put("requestId", requestId)
            .put("orderId", orderId)
            .put("pickupLat", pLat).put("pickupLng", pLng)
            .put("deliveryLat", dLat).put("deliveryLng", dLng)
            .put("weightKg", weight));
        currentDelivery = reply.get(20, TimeUnit.SECONDS);
    }

    @Then("the delivery is created with status {string}")
    public void delivery_created_with_status(String status) {
        assertThat(currentDelivery.getString("status")).isEqualTo(status);
    }

    @Then("the delivery has no drone assigned")
    public void delivery_has_no_drone() {
        assertThat(currentDelivery.getString("droneId")).isNull();
    }
}
