package io.github.muriloI.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.muriloI.avro.OrderAvro;
import io.github.muriloI.callback.StandardCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class AvroProducer {
    public static void main(String[] args) {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");

        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        // milliseconds to wait for the retry
        props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "500");
        // the producer receives acknowledgment from the broker after all replicas receive the msg
        // other values are 0: the message is lost if there is an error, and 1: when the leader
        // receives the message successfully only
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        // gzip uses more cpu, but the compression ratio is higher, snappy works in the opposite way
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        // number of bytes to go on a batch of messages
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        // the producer thread will hand over the message in the batch though a center thread asa
        // the center thread is free, unless you configure the linger ms to send it either way
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "500");
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "800");

        try (var producer = new KafkaProducer<Integer, OrderAvro>(props)) {
            var record2 = new ProducerRecord<>("OrderAvroTopic", 3,
                    new OrderAvro("Jeff", "MacBook Pro", 1));
            producer.send(record2, new StandardCallback());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}