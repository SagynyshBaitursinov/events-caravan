package io.saga.caravan.event.serialization;

import io.saga.caravan.event.EventType;

public interface EventPayloadDeserializer {

  Object deserializePayload(String payload,
                            EventType eventType) throws EventPayloadDeserializationException;
}
