package io.saga.caravan.event;

import java.util.Map;

public interface EntityEventsRegistration {

  String entityName();

  Map<String, Class<?>> eventToPayloadClass();

  boolean isSubscriptionActive();
}
