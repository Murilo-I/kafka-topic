package io.github.muriloI.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class StandardCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null)
            throw new RuntimeException(exception);

        System.out.printf("Topic: %s - Offset: %d - Partition: %d\n",
                metadata.topic(), metadata.offset(), metadata.partition());
    }
}
