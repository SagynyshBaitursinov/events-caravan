package io.saga.caravan.event;

import java.util.Map;

public record EntityEventsRegistration(String entityName,
                                       Map<String, Class<?>> eventToPayloadClass,
                                       boolean isSubscriptionActive) {
}
