package io.github.muriloI.producer;

import com.maxmind.geoip2.WebServiceClient;
import io.github.muriloI.callback.StandardCallback;
import io.github.muriloI.record.TruckLocation;
import io.github.muriloI.serial.TruckLocationSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.net.InetAddress;
import java.util.Properties;

public class TruckLocationProducer {
    public static void main(String[] args) {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", IntegerSerializer.class.getName());
        props.setProperty("value.serializer", TruckLocationSerializer.class.getName());

        try (var producer = new KafkaProducer<Integer, TruckLocation>(props);
             var client = new WebServiceClient.Builder(42, "license_key").build()) {

            var location = client.city(InetAddress.getLocalHost()).getLocation();

            var record = new ProducerRecord<>("TruckCSLocation", 144,
                    new TruckLocation(14, location.getLatitude(), location.getLongitude()));
            producer.send(record, new StandardCallback());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}