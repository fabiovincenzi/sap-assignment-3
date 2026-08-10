package sap.shipping.gateway.infrastructure;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import sap.shipping.common.exagonal.Adapter;

/**
 * Application Metrics for the API Gateway. Having no domain core to observe, it measures
 * incoming requests, fed by a routing handler (see GatewayController).
 *
 * The two metrics are what the service level indicators are computed from, see
 * doc/service-levels.md.
 */
@Adapter
public class GatewayMetrics {

    /** A request is successful when the gateway answers with a status below 400. */
    private static final int FIRST_ERROR_STATUS = 400;

    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    private final Counter requestsTotal;
    private final Histogram requestDuration;
    private final HTTPServer promServer;

    public GatewayMetrics(int port) throws ObsMetricServerException {
        JvmMetrics.builder().register();

        requestsTotal = Counter.builder()
            .name("gateway_requests_total")
            .help("Total number of HTTP requests answered by the gateway, by outcome")
            .labelNames("outcome")
            .register();

        requestDuration = Histogram.builder()
            .name("gateway_request_duration_seconds")
            .help("Time taken by the gateway to answer a request")
            /* the bound at 1 second is the one the latency objective is stated against */
            .classicUpperBounds(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
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
        requestsTotal.labelValues(statusCode < FIRST_ERROR_STATUS ? "success" : "error").inc();
        requestDuration.observe(durationNanos / NANOS_PER_SECOND);
    }
}
