package sap.shipping.delivery.infrastructure;

import java.util.logging.Level;
import java.util.logging.Logger;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import sap.shipping.common.exagonal.Adapter;

/**
 * What is left of the HTTP surface: the health endpoint alone, since every domain operation
 * arrives on an event channel. It stays over HTTP because the platform probes it that way.
 */
@Adapter
public class DeliveryController {

    static Logger logger = Logger.getLogger("[Delivery Service Controller]");

    public DeliveryController() {
        logger.setLevel(Level.INFO);
    }

    public Router createRouter(Vertx vertx) {
        var router = Router.router(vertx);
        router.get("/health").handler(this::healthCheck);
        return router;
    }

    /**
     * Health Check API (observability pattern). Reports whether the service is
     * able to handle requests. The repository is in-memory, so there is no
     * external dependency to probe.
     */
    private void healthCheck(RoutingContext ctx) {
        var reply = new JsonObject()
            .put("status", "UP")
            .put("checks", new JsonArray());
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(reply.encode());
    }
}
