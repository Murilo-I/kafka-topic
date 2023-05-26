package io.github.muriloI.partition;

import io.confluent.common.utils.Utils;
import io.github.muriloI.record.TruckLocation;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class VIPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        TruckLocation location = (TruckLocation) value;

        if (location.latitude().equals(37.2431) && location.longitude().equals(115.793))
            return 5;

        return Math.abs(Utils.murmur2(keyBytes)) % partitions.size() - 1;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
