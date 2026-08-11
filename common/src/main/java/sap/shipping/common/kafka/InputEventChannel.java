package sap.shipping.common.kafka;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Incoming event channel, subscribed to a Kafka topic named after the channel.
 *
 * The consumer group is a parameter: consumers sharing a group split the messages
 * between them, so two services subscribed to the same topic under one group would
 * steal each other's events. One group per service means every service sees everything.
 */
public class InputEventChannel {

    static Logger logger = Logger.getLogger("[InputEventChannel]");

    private final String name;
    private final KafkaConsumer<String, String> consumer;

    public InputEventChannel(Vertx vertx, String name, String address, String consumerGroup) {
        this.name = name;
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", address);
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", consumerGroup);
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "true");
        this.consumer = KafkaConsumer.create(vertx, config);
        this.consumer.pollTimeout(Duration.ofMillis(100));
    }

    public Future<Void> init(Consumer<JsonObject> handler) {
        consumer.handler(record -> {
            logger.info("[" + name + "] received key=" + record.key() + " offset=" + record.offset());
            handler.accept(new JsonObject(record.value()));
        });
        return consumer.subscribe(name)
            .onSuccess(v -> logger.info("[" + name + "] subscribed"))
            .onFailure(cause -> logger.warning("[" + name + "] subscribe failed: " + cause.getMessage()));
    }

    public Future<Void> close() {
        return consumer.close();
    }
}
