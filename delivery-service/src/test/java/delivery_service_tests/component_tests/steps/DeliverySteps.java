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
import sap.shipping.delivery.infrastructure.KafkaDeliveryServiceObserver;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the whole delivery service through its event channels, the way a caller would: the
 * service is started for real, only the broker is shared with the environment.
 *
 * Announcing a confirmed order returns nothing, so the outcome is observed on the announcement
 * of the new delivery, and its state is then read back over the query channel.
 */
public class DeliverySteps {

    private static final String ORDER_CONFIRMED = "order-confirmed";
    private static final String NEW_DELIVERY_CREATED = "new-delivery-created";
    private static final String GET_REQUESTS = "get-delivery-requests";
    private static final String GET_APPROVED = "get-delivery-requests-approved";

    private static final int TIMEOUT_SECONDS = 20;

    private Vertx vertx;
    private OutputEventChannel orderConfirmed;
    private OutputEventChannel getRequests;

    private final CompletableFuture<JsonObject> announcement = new CompletableFuture<>();
    private final CompletableFuture<JsonObject> delivery = new CompletableFuture<>();

    private String orderId;
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
        service.addObserver(new KafkaDeliveryServiceObserver(vertx, broker));

        vertx.deployVerticle(new DeliveryServiceEventBasedController(service, broker,
                "component-test-" + UUID.randomUUID()))
            .toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        orderId = "order-" + UUID.randomUUID();
        requestId = UUID.randomUUID().toString();

        orderConfirmed = new OutputEventChannel(vertx, ORDER_CONFIRMED, broker);
        getRequests = new OutputEventChannel(vertx, GET_REQUESTS, broker);

        subscribe(broker, NEW_DELIVERY_CREATED, event -> {
            if (orderId.equals(event.getString("orderId"))) {
                announcement.complete(event);
            }
        });
        subscribe(broker, GET_APPROVED, event -> {
            if (requestId.equals(event.getString("requestId"))) {
                delivery.complete(event);
            }
        });
    }

    /** A group of its own per run, so the test sees every event instead of a share of them. */
    private void subscribe(String broker, String channel, java.util.function.Consumer<JsonObject> handler)
            throws Exception {
        new InputEventChannel(vertx, channel, broker, "component-test-" + UUID.randomUUID())
            .init(handler)
            .toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @After
    public void stopTheService() {
        if (vertx != null) {
            vertx.close();
        }
    }

    @When("the order {string} is confirmed, from \\({double}, {double}) to \\({double}, {double}) weighing {double} kg")
    public void the_order_is_confirmed(String ignoredName, double pLat, double pLng,
                                       double dLat, double dLng, double weight) throws Exception {
        orderConfirmed.postEvent(orderId, new JsonObject()
            .put("orderId", orderId)
            .put("pickupLat", pLat).put("pickupLng", pLng)
            .put("deliveryLat", dLat).put("deliveryLng", dLng)
            .put("weightKg", weight));

        announcement.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        /* asked by order, which is unique per run: the broker is shared with whatever else is
           running, and only the instance that owns this delivery answers on the approved channel */
        getRequests.postEvent(requestId, new JsonObject()
            .put("requestId", requestId)
            .put("orderId", orderId));
        currentDelivery = delivery.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Then("a delivery is announced with status {string}")
    public void delivery_announced_with_status(String status) {
        assertThat(currentDelivery.getString("status")).isEqualTo(status);
    }

    @Then("the delivery has no drone assigned")
    public void delivery_has_no_drone() {
        assertThat(currentDelivery.getString("droneId")).isNull();
    }
}
