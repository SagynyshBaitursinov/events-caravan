package io.saga.caravan.test.event;

public record TestEventPayload(String testField,
                               String anotherTestField) implements TestFieldContainingEventPayload {

}
