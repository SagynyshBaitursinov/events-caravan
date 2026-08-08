package dev.baitursinov.caravan.test.event.driven;

public record TestEventPayload(String testField,
                               String anotherTestField) implements TestFieldContainingEventPayload {

}
