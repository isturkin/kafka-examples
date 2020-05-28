package com.visseri.examples.kafka.callback;

import com.visseri.examples.kafka.model.Application;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import static java.util.Objects.isNull;

@Slf4j
@RequiredArgsConstructor
public class MessageCallback implements Callback {

    private final Application data;

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (isNull(exception)) {
            log.info("Sending was completed for: " + data);
        }
    }
}
