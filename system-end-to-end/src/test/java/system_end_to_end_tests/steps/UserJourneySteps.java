package system_end_to_end_tests.steps;

import io.cucumber.java.en.*;
import io.vertx.core.json.JsonObject;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.*;

/**
 * End-to-end test: every service runs for real, in its own container, and the journey goes
 * only through the API gateway - the same entry point a client would use.
 *
 * The system must be up before running it: docker compose up
 */
public class UserJourneySteps {

    private static final String GATEWAY = "http://localhost:8080";
    private static final String API = GATEWAY + "/api/v1";

    /** How long the choreography is given to settle before the journey gives up. */
    private static final Duration SETTLING_BUDGET = Duration.ofSeconds(20);
    private static final long POLL_INTERVAL_MILLIS = 500;

    private JsonObject order;
    private JsonObject tracking;

    @Given("the shipping system is running")
    public void theSystemIsRunning() {
        HttpResponse<String> response;
        try {
            response = get(GATEWAY + "/health");
        } catch (Exception e) {
            throw new AssertionError("the gateway is not reachable at " + GATEWAY
                + ": start the system with 'docker compose up' before running the end-to-end tests", e);
        }
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(new JsonObject(response.body()).getString("status")).isEqualTo("UP");
    }

    @Given("a drone able to carry {double} kg is available")
    public void aDroneIsAvailable(double maxWeightKg) throws Exception {
        var body = new JsonObject().put("maxWeightKg", maxWeightKg).put("lat", 44.14).put("lng", 12.24);

        var response = post(API + "/drones", body);

        assertThat(response.statusCode()).isEqualTo(201);
    }

    @When("I register as {string} and place an order weighing {double} kg")
    public void iRegisterAndPlaceAnOrder(String username, double weightKg) throws Exception {
        /* a unique username, so that the journey can be replayed on a system already used */
        var uniqueUsername = username + "-" + UUID.randomUUID().toString().substring(0, 8);
        var registration = new JsonObject().put("username", uniqueUsername).put("password", "Secret#123");
        var registered = post(API + "/users/register", registration);
        assertThat(registered.statusCode()).isEqualTo(201);
        var customerId = new JsonObject(registered.body()).getString("userId");

        var newOrder = new JsonObject()
            .put("customerId", customerId)
            .put("pickupStreet", "Via Roma 1").put("pickupLat", 44.14).put("pickupLng", 12.24)
            .put("deliveryStreet", "Via Verdi 9").put("deliveryLat", 44.15).put("deliveryLng", 12.25)
            .put("weightKg", weightKg);

        var response = post(API + "/orders", newOrder);
        assertThat(response.statusCode()).isEqualTo(201);
        order = new JsonObject(response.body());
    }

    @Then("the order is created with status {string}")
    public void theOrderIsCreatedWithStatus(String status) {
        assertThat(order.getString("status")).isEqualTo(status);
    }

    @When("I confirm the order")
    public void iConfirmTheOrder() throws Exception {
        var response = post(API + "/orders/" + order.getString("id") + "/confirm", null);

        assertThat(response.statusCode()).isEqualTo(200);
    }

    @Then("the tracking of my order reports a delivery")
    public void theTrackingReportsADelivery() throws Exception {
        tracking = awaitTracking(t -> Boolean.TRUE.equals(t.getBoolean("hasDelivery")),
            "confirming the order must have triggered a delivery");
    }

    @Then("the delivery eventually has a drone assigned")
    public void theDeliveryEventuallyHasADroneAssigned() throws Exception {
        tracking = awaitTracking(t -> "DRONE_ASSIGNED".equals(t.getString("deliveryStatus")),
            "the fleet must answer the announced delivery with a drone");
        assertThat(tracking.getString("droneId")).isNotBlank();
    }

    /**
     * Confirming an order no longer produces the whole outcome at once: the delivery and its
     * drone arrive as answers to announced facts, so the journey polls instead of asserting on
     * the spot.
     */
    private JsonObject awaitTracking(Predicate<JsonObject> settled, String what) throws Exception {
        var deadline = Instant.now().plus(SETTLING_BUDGET);
        JsonObject last = null;
        while (Instant.now().isBefore(deadline)) {
            var response = get(API + "/tracking/" + order.getString("id"));
            assertThat(response.statusCode()).isEqualTo(200);
            last = new JsonObject(response.body());
            if (settled.test(last)) {
                return last;
            }
            Thread.sleep(POLL_INTERVAL_MILLIS);
        }
        throw new AssertionError(what + ", but after " + SETTLING_BUDGET.toSeconds()
            + "s the tracking was still " + last);
    }

    private HttpResponse<String> get(String url) throws Exception {
        var request = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
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
