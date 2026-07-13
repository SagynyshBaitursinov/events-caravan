package io.saga.caravan.event.payload;

import java.util.Map;

public interface EventPayloadRegistration {

  String entityName();

  Map<String, Class<?>> eventToPayloadClass();

  boolean isIncomingSubscriptionActive();
}
