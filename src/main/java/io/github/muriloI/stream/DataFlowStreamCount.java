package io.github.muriloI.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.List;
import java.util.Properties;

public class DataFlowStreamCount {
    public static void main(String[] args) {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-dataflow");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        var serde = Serdes.String().getClass().getName();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, serde);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, serde);
        // only for small test application
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        var builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("streams-count-input");
        stream.foreach((key, value) -> System.out.println("Key: " + key + " - Value: " + value));
        stream.flatMapValues(value -> List.of(value.toLowerCase().split(" ")))
                .groupBy((key, value) -> value).count().toStream()
                .to("streams-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        var topology = builder.build();
        System.out.println(topology.describe());

        try (var streams = new KafkaStreams(topology, props)) {
            streams.cleanUp();
            streams.start();
//            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }
    }
}
