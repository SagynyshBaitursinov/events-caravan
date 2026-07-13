package io.saga.caravan.event;

public record TestEventPayload(String testField,
                               String anotherTestField) implements TestFieldContainingEventPayload {

}
