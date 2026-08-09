package dev.baitursinov.caravan.test.value;

public record TestEventPayload(String testField,
                               String anotherTestField) implements TestFieldContainingEventPayload {

}
