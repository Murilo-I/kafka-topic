package io.github.muriloI.serial;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.muriloI.record.Order;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class OrderDeserializer implements Deserializer<Order> {
    @Override
    public Order deserialize(String topic, byte[] data) {
        var mapper = new ObjectMapper();
        try {
            return mapper.readValue(data, Order.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
