package com.visseri.examples.kafka.producer;

import com.github.javafaker.Faker;
import com.google.gson.Gson;
import com.visseri.examples.kafka.callback.MessageCallback;
import com.visseri.examples.kafka.model.Application;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static com.google.common.io.Resources.getResource;

@Slf4j
public class HRInformer {

    private static final String TOPIC_NAME = "applications";

    public static void main(String[] args) throws Exception {
        Properties producerProperties = new Properties();
        try(InputStream is = getResource("producer.properties").openStream()) {
                producerProperties.load(is);
        } catch (Exception exception) {
            log.error("Error was occurred during loading producer settings", exception);
        }

        Faker faker = new Faker();
        Gson gson = new Gson();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties);
        for (int i = 0; i < 100; i++) {
            Thread.sleep(1000L);
            Application application = new Application();
            application.setUid(UUID.randomUUID().toString());
            application.setFullName(faker.name().fullName());
            application.setExpectedSalary(new Random().nextDouble() * 5_000);
            kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, application.getUid(),
                            gson.toJson(application)), new MessageCallback(application));
        }
    }

}
