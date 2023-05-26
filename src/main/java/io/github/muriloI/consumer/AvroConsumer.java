package io.github.muriloI.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.github.muriloI.avro.OrderAvro;
import io.github.muriloI.handler.RebalanceHandler;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class AvroConsumer {
    public static void main(String[] args) {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");
        props.setProperty("specific.avro.reader", "true");
        props.setProperty("group.id", "OrderGroup");
        props.setProperty("auto.commit.offset", "false");

        // read the records from the beginning of the partition,
        // other: latest, read only the ones that was sent after the consumer started
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        // equally distribute the topic partitions among the consumers, default: RangeAssignor
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                RoundRobinAssignor.class.getName());

        try (var consumer = new KafkaConsumer<Integer, OrderAvro>(props)) {
            var currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
            consumer.subscribe(Collections.singleton("OrderAvroTopic"),
                    new RebalanceHandler(consumer, currentOffsets));
            int count = 0;
            while (count <= 100) {
                var orders = consumer.poll(Duration.ofSeconds(30));

                for (ConsumerRecord<Integer, OrderAvro> order : orders) {
                    System.out.printf("Product Name: %s - Quantity: %d\n",
                            order.value().getProduct(), order.value().getQuantity());

                    currentOffsets.put(new TopicPartition(order.topic(), order.partition()),
                            new OffsetAndMetadata(order.offset() + 1));

                    if (count % 10 == 0) {
                        consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                            if (exception != null)
                                System.out.println("Commit failed for offset " + offsets);
                        });
                    }
                    count++;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}