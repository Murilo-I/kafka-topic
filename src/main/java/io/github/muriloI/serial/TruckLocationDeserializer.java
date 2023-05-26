package io.github.muriloI.serial;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.muriloI.record.TruckLocation;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class TruckLocationDeserializer implements Deserializer<TruckLocation> {
    @Override
    public TruckLocation deserialize(String topic, byte[] data) {
        var mapper = new ObjectMapper();
        try {
            return mapper.readValue(data, TruckLocation.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
