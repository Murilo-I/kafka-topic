package io.github.muriloI.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class SimpleConsumer {
    public static void main(String[] args) {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");
        props.setProperty("auto.offset.reset", "earliest");

        try (var consumer = new KafkaConsumer<Integer, GenericRecord>(props)) {
            String topic = "SimpleTopic";
            var partitionsInfo = consumer.partitionsFor(topic);
            var partitions = new ArrayList<TopicPartition>();

            partitionsInfo.forEach(info -> partitions.add(new TopicPartition(topic, info.partition())));

            consumer.assign(partitions);

            var orders = consumer.poll(Duration.ofSeconds(30));
            orders.forEach(order -> System.out.printf("Product Name: %s - Quantity: %s\n",
                    order.value().get("product"), order.value().get("quantity")));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}