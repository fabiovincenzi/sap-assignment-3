package delivery_service_tests.component_tests.steps;

import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.*;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import delivery_service_tests.infrastructure.Synchroniser;
import sap.shipping.delivery.application.DroneServicePort;
import sap.shipping.delivery.application.OrderServicePort;
import sap.shipping.delivery.application.DeliveryServiceImpl;
import sap.shipping.delivery.infrastructure.DeliveryController;
import sap.shipping.delivery.infrastructure.EventSourcedDeliveryRepository;
import sap.shipping.delivery.infrastructure.InMemoryDeliveryEventStore;
import sap.shipping.delivery.infrastructure.InMemoryDeliveryLookupView;
import sap.shipping.delivery.infrastructure.InMemoryDeliverySnapshotStore;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * Component test: the delivery service is started whole - controller, application and
 * event-sourced persistence - and driven from the outside through its HTTP API, exactly
 * as a client would. Only the other services are stubbed.
 */
public class DeliverySteps {

    private static final int SERVICE_PORT = 9594;
    private static final String DELIVERIES_ENDPOINT = "http://localhost:" + SERVICE_PORT + "/api/deliveries";

    private Vertx vertx;
    private JsonObject currentDelivery;

    @Before
    public void startTheService() throws Exception {
        var sync = new Synchroniser();
        vertx = Vertx.vertx();

        var repository = new EventSourcedDeliveryRepository(new InMemoryDeliveryEventStore(),
            new InMemoryDeliverySnapshotStore(), new InMemoryDeliveryLookupView());

        /* no stub of the neighbouring services is needed any more: the delivery service does
           not call anyone, it announces facts */
        var service = new DeliveryServiceImpl(repository);
        var controller = new DeliveryController(service);

        vertx.createHttpServer()
            .requestHandler(controller.createRouter(vertx))
            .listen(SERVICE_PORT)
            .onSuccess(server -> sync.notifySync());
        sync.awaitSync();
    }

    @After
    public void stopTheService() {
        vertx.close();
    }

    @When("a delivery is scheduled for order {string} from \\({double}, {double}) to \\({double}, {double}) weighing {double} kg")
    public void schedule_delivery(String orderId, double pLat, double pLng, double dLat, double dLng, double weight) throws Exception {
        var body = new JsonObject()
            .put("orderId", orderId)
            .put("pickupLat", pLat).put("pickupLng", pLng)
            .put("deliveryLat", dLat).put("deliveryLng", dLng)
            .put("weightKg", weight);
        currentDelivery = new JsonObject(post(DELIVERIES_ENDPOINT, body).body());
    }

    @Then("the delivery is created with status {string}")
    public void delivery_created_with_status(String status) {
        assertThat(currentDelivery.getString("status")).isEqualTo(status);
    }

    @Then("the delivery has no drone assigned")
    public void delivery_has_no_drone() {
        assertThat(currentDelivery.getString("droneId")).isNull();
    }

    private HttpResponse<String> post(String url, JsonObject body) throws Exception {
        var publisher = body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(body.encode());
        var request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .POST(publisher)
            .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }
}
