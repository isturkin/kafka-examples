package com.visseri.examples.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;

import static com.google.common.io.Resources.getResource;
import static com.visseri.examples.kafka.constants.KafkaConstants.APPLICATIONS_TOPIC_NAME;
import static java.util.Collections.singletonList;

@Slf4j
public class HRStatisticsPrinter {

    public static void main(String[] args) {
        log.info("Starting Kafka consumer...");
        Properties consumerProperties = new Properties();
        try(InputStream is = getResource("consumer.properties").openStream()) {
            consumerProperties.load(is);
        } catch (Exception exception) {
            log.error("Error occurred during loading consumer properties", exception);
        }
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
        kafkaConsumer.subscribe(singletonList(APPLICATIONS_TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                log.info("New application was received with key:{} and value:{}",
                        record.key(), record.value());
            }
        }
    }
}
