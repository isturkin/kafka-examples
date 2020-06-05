package com.visseri.examples.kafka.streams.jobs;

import com.visseri.examples.kafka.model.Application;
import com.visseri.examples.kafka.serde.CustomSerde;
import com.visseri.examples.kafka.streams.processor.PositionFilter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static com.visseri.examples.kafka.constants.KafkaConstants.APPLICATIONS_TOPIC_NAME;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Slf4j
public class ApplicationsJob {

    public static void main(String[] args) {
        String desiredPosition = args[0];
        if (isNotBlank(desiredPosition)) {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-applications-input");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomSerde.class);

            StreamsBuilder streamsBuilder = new StreamsBuilder();
            KStream<String, Application> stream = streamsBuilder.stream(APPLICATIONS_TOPIC_NAME);

            stream.filter(new PositionFilter(desiredPosition))
                    .mapValues(Application::getFullName)
                    .to("streams-potential-candidates-output");

            KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
            kafkaStreams.start();
        } else {
            log.error("Please input a desired job position to find only appropriate candidates");
        }
    }
}
