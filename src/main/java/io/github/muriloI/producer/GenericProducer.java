package io.github.muriloI.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.muriloI.callback.StandardCallback;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.Properties;

public class GenericProducer {
    public static void main(String[] args) {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");

        try (var producer = new KafkaProducer<Integer, GenericRecord>(props)) {
            var parser = new Schema.Parser();
            var schema = parser.parse(new File("./src/main/resources/order.avsc"));
            GenericRecord order = new GenericData.Record(schema);
            order.put("customerName", "John");
            order.put("product", "Galaxy S23 Ultra");
            order.put("quantity", 1);

            var record = new ProducerRecord<>("SimpleTopic", 4, order);
            producer.send(record, new StandardCallback());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}