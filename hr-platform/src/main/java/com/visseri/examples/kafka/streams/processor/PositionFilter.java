package com.visseri.examples.kafka.streams.processor;


import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.kstream.Predicate;

public class PositionFilter implements Predicate<String, JsonNode> {

    private final String position;

    public PositionFilter(String position) {
        this.position = position;
    }

    @Override
    public boolean test(String key, JsonNode value) {
        JsonNode actual = value.path("position");
        if (!actual.isMissingNode()) {
            return position.equals(actual.textValue());
        }
        return false;
    }
}
