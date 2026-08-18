package sap.shipping.common.kafka;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.List;
import java.util.Properties;

/** Admin operations on the event channels, used to reset them between tests. */
public class EventChannelsAdmin {

    private final KafkaAdminClient adminClient;

    public EventChannelsAdmin(Vertx vertx, String address) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, address);
        this.adminClient = KafkaAdminClient.create(vertx, config);
    }

    public Future<Void> purgeChannel(String channelName) {
        return adminClient.deleteTopics(List.of(channelName));
    }

    public Future<Void> close() {
        return adminClient.close();
    }
}
