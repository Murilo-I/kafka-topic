package io.github.muriloI.consumer;

import io.github.muriloI.record.Order;
import io.github.muriloI.serial.OrderDeserializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {
    public static void main(String[] args) {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", IntegerDeserializer.class.getName());
        props.setProperty("value.deserializer", OrderDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");

        try (var consumer = new KafkaConsumer<Integer, Order>(props)) {
            consumer.subscribe(Collections.singleton("OrderCSTopic"));
            var orders = consumer.poll(Duration.ofSeconds(30));
            orders.forEach(order -> System.out.printf("Product Name: %s - Quantity: %d\n",
                    order.value().product(), order.value().quantity()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}