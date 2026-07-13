package io.saga.caravan.event.serialization;

import io.saga.caravan.event.Event;

public interface EventPayloadSerializer {

  String serializePayload(Event<?> event);
}
