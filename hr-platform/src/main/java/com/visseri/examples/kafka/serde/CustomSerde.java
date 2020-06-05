package com.visseri.examples.kafka.serde;

import com.google.gson.Gson;
import com.visseri.examples.kafka.model.Application;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomSerde<T> implements Serde<T>, Serializer<T>, Deserializer<T> {

    private final Gson gson = new Gson();

    @Override
    public T deserialize(String topic, byte[] data) {
        return (T) gson.fromJson(new String(data), Application.class);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        return gson.toJson(data).getBytes();
    }

}
