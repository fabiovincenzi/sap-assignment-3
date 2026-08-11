package sap.shipping.common.kafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Outgoing event channel, backed by a Kafka topic named after the channel.
 *
 * Every event carries a partition key: Kafka only guarantees ordering within a
 * partition, and records without a key are spread across all of them.
 */
public class OutputEventChannel {

    static Logger logger = Logger.getLogger("[OutputEventChannel]");

    private final String name;
    private final KafkaProducer<String, String> producer;

    public OutputEventChannel(Vertx vertx, String name, String address) {
        this.name = name;
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", address);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "1");
        this.producer = KafkaProducer.create(vertx, config);
    }

    /**
     * Posts an event, keyed so that every event about the same entity lands on the same
     * partition and is therefore delivered in order.
     */
    public Future<Void> postEvent(String key, JsonObject event) {
        var promise = Promise.<Void>promise();
        event.put("timestamp", System.currentTimeMillis());
        var record = KafkaProducerRecord.<String, String>create(name, key, event.encode());
        producer.send(record)
            .onSuccess(metadata -> {
                logger.info("[" + name + "] posted key=" + key
                    + " partition=" + metadata.getPartition() + " offset=" + metadata.getOffset());
                promise.complete();
            })
            .onFailure(cause -> {
                logger.warning("[" + name + "] post failed: " + cause.getMessage());
                promise.fail(cause);
            });
        return promise.future();
    }

    public Future<Void> close() {
        return producer.close();
    }
}
