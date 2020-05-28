package com.visseri.examples.kafka.streams.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import com.visseri.examples.kafka.serde.CustomJsonSerde;
import com.visseri.examples.kafka.streams.processor.PositionFilter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

import static com.visseri.examples.kafka.constants.KafkaConstants.APPLICATIONS_TOPIC_NAME;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Slf4j
public class ApplicationsJob {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-applications-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomJsonSerde.class);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, JsonNode> stream = streamsBuilder.stream(APPLICATIONS_TOPIC_NAME);

        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        String desiredPosition = args[0];
        if (isNotBlank(desiredPosition)) {

            KTable<String, Long> counts = stream
                    .filter(new PositionFilter(desiredPosition))
                    .map(((key, value) -> new KeyValue<>(key, value.asText())))
                    .groupBy((key, value) -> value)
                    .count();
            counts.toStream().to("streams-potential-candidates");

            kafkaStreams.start();

            Thread.sleep(600_000);

            kafkaStreams.close();
        } else {
            log.error("Please input a desired job position to find appropriate candidate");
        }
    }
}
