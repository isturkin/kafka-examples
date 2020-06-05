package com.visseri.examples.kafka.streams.processor;


import com.visseri.examples.kafka.model.Application;
import org.apache.kafka.streams.kstream.Predicate;

import static java.util.Objects.nonNull;

public class PositionFilter implements Predicate<String, Application> {

    private final String position;

    public PositionFilter(String position) {
        this.position = position;
    }

    @Override
    public boolean test(String key, Application application) {
        if (nonNull(application)) {
            return position.equals(application.getPosition());
        }
        return false;
    }
}
