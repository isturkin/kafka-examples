package com.visseri.examples.kafka.serde;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Map;

public class CustomJsonSerde implements Serde<JsonNode>, Serializer<JsonNode>, Deserializer<JsonNode> {

    private final JsonSerializer jsonSerializer = new JsonSerializer();
    private final JsonDeserializer jsonDeserializer = new JsonDeserializer();

    @Override
    public JsonNode deserialize(String topic, byte[] data) {
        return jsonDeserializer.deserialize(topic, data);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<JsonNode> serializer() {
        return this;
    }

    @Override
    public Deserializer<JsonNode> deserializer() {
        return this;
    }

    @Override
    public byte[] serialize(String topic, JsonNode data) {
        return jsonSerializer.serialize(topic, data);
    }
}
