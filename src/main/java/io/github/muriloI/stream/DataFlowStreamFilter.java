package io.github.muriloI.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class DataFlowStreamFilter {
    public static void main(String[] args) {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-dataflow");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        var serde = Serdes.Integer().getClass().getName();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, serde);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, serde);

        var builder = new StreamsBuilder();
        KStream<Integer, Integer> stream = builder.stream("streams-dataflow-input");
        stream.foreach((key, value) -> System.out.println("Key: " + key + " - Value: " + value));
        stream.filter((key, value) -> value % 2 == 1)
                .map((key, value) -> KeyValue.pair(key, value * 3))
                .to("streams-dataflow-output");

        var topology = builder.build();
        System.out.println(topology.describe());

        try (var streams = new KafkaStreams(topology, props)) {
            streams.cleanUp();
            streams.start();
//            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        }
    }
}
