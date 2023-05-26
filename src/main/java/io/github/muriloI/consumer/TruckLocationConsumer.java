package io.github.muriloI.consumer;

import io.github.muriloI.record.TruckLocation;
import io.github.muriloI.serial.TruckLocationDeserializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TruckLocationConsumer {
    public static void main(String[] args) {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", IntegerDeserializer.class.getName());
        props.setProperty("value.deserializer", TruckLocationDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");

        try (var consumer = new KafkaConsumer<Integer, TruckLocation>(props)) {
            consumer.subscribe(Collections.singleton("TruckCSLocation"));
            var orders = consumer.poll(Duration.ofSeconds(30));
            orders.forEach(order -> System.out.printf("TruckId: %d - Coordinates: %f,%f\n",
                    order.value().truckId(), order.value().latitude(), order.value().longitude()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}