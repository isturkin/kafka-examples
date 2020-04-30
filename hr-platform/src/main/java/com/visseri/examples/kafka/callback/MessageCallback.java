package com.visseri.examples.kafka.callback;

import com.visseri.examples.kafka.model.Application;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import static java.util.Objects.nonNull;

@RequiredArgsConstructor
public class MessageCallback implements Callback {

    private final Application data;

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (nonNull(exception)) {
            System.out.println("Sending was completed for: " + data);
        }
    }
}
