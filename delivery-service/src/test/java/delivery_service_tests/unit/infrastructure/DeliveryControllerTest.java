package delivery_service_tests.unit.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sap.shipping.delivery.infrastructure.DeliveryController;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The HTTP surface of the delivery service is the health endpoint alone: every domain
 * operation arrives on an event channel. What is left to check here is that the platform can
 * still probe the service.
 */
public class DeliveryControllerTest {

    private static final int TEST_PORT = 8899;

    private Vertx vertx;

    @BeforeEach
    public void setUp() throws Exception {
        var sync = new Synchroniser();
        vertx = Vertx.vertx();
        var controller = new DeliveryController();

        vertx.createHttpServer()
            .requestHandler(controller.createRouter(vertx))
            .listen(TEST_PORT)
            .onSuccess(server -> sync.notifySync());

        sync.awaitSync();
    }

    @AfterEach
    public void tearDown() {
        vertx.close();
    }

    @Test
    public void theHealthEndpointReportsTheServiceIsUp() throws Exception {
        var response = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder().uri(URI.create("http://localhost:" + TEST_PORT + "/health")).GET().build(),
            HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(new JsonObject(response.body()).getString("status")).isEqualTo("UP");
    }

    @Test
    public void theDomainRoutesAreGone() throws Exception {
        var response = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder().uri(URI.create("http://localhost:" + TEST_PORT + "/api/deliveries/any")).GET().build(),
            HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(404);
    }
}
