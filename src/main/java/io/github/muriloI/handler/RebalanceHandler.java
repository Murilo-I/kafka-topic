package io.github.muriloI.handler;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

public class RebalanceHandler implements ConsumerRebalanceListener {

    private final KafkaConsumer<?, ?> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    public RebalanceHandler(KafkaConsumer<?, ?> consumer, Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        this.consumer = consumer;
        this.currentOffsets = currentOffsets;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        consumer.commitSync(currentOffsets);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

    }
}
