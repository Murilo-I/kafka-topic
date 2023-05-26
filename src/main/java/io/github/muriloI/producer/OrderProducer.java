package io.github.muriloI.producer;

import io.github.muriloI.callback.StandardCallback;
import io.github.muriloI.record.Order;
import io.github.muriloI.serial.OrderSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", IntegerSerializer.class.getName());
        props.setProperty("value.serializer", OrderSerializer.class.getName());

        try (var producer = new KafkaProducer<Integer, Order>(props)) {
            String topicName = "OrderCSTopic";

            //sync way
            var record = new ProducerRecord<>(topicName, 1,
                    new Order("Leon", "Laptop ThinkPad", 2));
            var metadata = producer.send(record).get();
            System.out.printf("Topic: %s - Offset: %d - Partition: %d\n",
                    metadata.topic(), metadata.offset(), metadata.partition());

            //async way
            var record2 = new ProducerRecord<>(topicName, 2,
                    new Order("Jeff", "MacBook Pro", 1));
            producer.send(record2, new StandardCallback());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}