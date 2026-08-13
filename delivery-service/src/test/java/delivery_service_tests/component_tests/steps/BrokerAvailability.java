package delivery_service_tests.component_tests.steps;

import org.junit.jupiter.api.Assumptions;

import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * The delivery service is reachable only through event channels, so a component test of it
 * needs a broker. When there is none the scenario is skipped rather than failed, so that the
 * ordinary build stays green with or without the infrastructure running.
 */
public final class BrokerAvailability {

    private BrokerAvailability() {
    }

    public static String address() {
        var configured = System.getenv("EV_CHANNELS_LOCATION");
        return configured == null || configured.isBlank() ? "localhost:29092" : configured;
    }

    public static void assumeReachable() {
        var parts = address().split(":");
        try (var socket = new Socket()) {
            socket.connect(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])), 1500);
        } catch (Exception unreachable) {
            Assumptions.abort("no broker at " + address() + ", skipping the channel component test");
        }
    }
}
