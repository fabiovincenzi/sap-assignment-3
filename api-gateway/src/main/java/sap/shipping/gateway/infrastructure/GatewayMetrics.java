package sap.shipping.gateway.infrastructure;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import sap.shipping.common.exagonal.Adapter;

/**
 * Application Metrics for the API Gateway. Having no domain core to observe, it measures
 * incoming requests, fed by a routing handler (see GatewayController).
 *
 * These metrics are what the service level indicators are computed from, see
 * doc/service-levels.md.
 */
@Adapter
public class GatewayMetrics {

    /** A request is successful when the gateway answers with a status below 400. */
    private static final int FIRST_ERROR_STATUS = 400;

    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    private final Counter requestsTotal;
    private final Counter requestsSuccessful;
    private final Counter responseTimeTotal;
    private final HTTPServer promServer;

    public GatewayMetrics(int port) throws ObsMetricServerException {
        JvmMetrics.builder().register();

        requestsTotal = Counter.builder()
            .name("gateway_requests_total")
            .help("Total number of HTTP requests answered by the gateway")
            .register();

        requestsSuccessful = Counter.builder()
            .name("gateway_successful_requests_total")
            .help("Requests answered with a status below 400")
            .register();

        /* accumulated, not per request: divided by the request count it gives the average
           time an answer took */
        responseTimeTotal = Counter.builder()
            .name("gateway_response_time_seconds_total")
            .help("Accumulated time taken by the gateway to answer requests")
            .register();

        try {
            promServer = HTTPServer.builder()
                .port(port)
                .buildAndStart();
        } catch (Exception ex) {
            throw new ObsMetricServerException();
        }
    }

    /** Records one answered request. Called when the response has been written. */
    public void observeRequest(int statusCode, long durationNanos) {
        requestsTotal.inc();
        if (statusCode < FIRST_ERROR_STATUS) {
            requestsSuccessful.inc();
        }
        responseTimeTotal.inc(durationNanos / NANOS_PER_SECOND);
    }
}
