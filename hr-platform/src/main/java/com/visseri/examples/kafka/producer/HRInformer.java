package com.visseri.examples.kafka.producer;

import com.github.javafaker.Faker;
import com.google.gson.Gson;
import com.visseri.examples.kafka.callback.MessageCallback;
import com.visseri.examples.kafka.model.Application;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.InputStream;
import java.util.*;

import static com.google.common.io.Resources.getResource;
import static com.visseri.examples.kafka.constants.KafkaConstants.APPLICATIONS_TOPIC_NAME;
import static java.util.Arrays.asList;

@Slf4j
public class HRInformer {

    private static final List<String> positions = asList(
            "Manager", "Scrum master", "Developer", "Testing engineer");

    public static void main(String[] args) throws Exception {
        log.info("Starting Kafka producer...");

        Properties producerProperties = new Properties();
        try(InputStream is = getResource("producer.properties").openStream()) {
                producerProperties.load(is);
        } catch (Exception exception) {
            log.error("Error occurred during loading producer settings", exception);
        }

        Faker faker = new Faker();
        Gson gson = new Gson();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties);
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000L);
            Application application = new Application();
            application.setUid(UUID.randomUUID().toString());
            application.setFullName(faker.name().fullName());
            application.setPosition(positions.get(random.nextInt(4)));
            application.setExpectedSalary(random.nextDouble() * 5_000);
            kafkaProducer.send(new ProducerRecord<>(APPLICATIONS_TOPIC_NAME, application.getUid(),
                            gson.toJson(application)), new MessageCallback(application));
        }
        log.info("Stopping Kafka producer...");
    }

}
