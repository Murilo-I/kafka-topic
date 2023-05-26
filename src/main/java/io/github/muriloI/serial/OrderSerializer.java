package io.github.muriloI.serial;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.muriloI.record.Order;
import org.apache.kafka.common.serialization.Serializer;

public class OrderSerializer implements Serializer<Order> {
    @Override
    public byte[] serialize(String topic, Order data) {
        var mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(data).getBytes();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
