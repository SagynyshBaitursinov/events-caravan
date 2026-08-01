package io.saga.caravan.event.serialization.jackson;

import io.saga.caravan.event.EntityEventsRegistry;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.serialization.EventPayloadDeserializationException;
import io.saga.caravan.event.serialization.EventPayloadDeserializer;
import lombok.RequiredArgsConstructor;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@RequiredArgsConstructor
public class JacksonEventPayloadDeserializer implements EventPayloadDeserializer {

  private final JsonMapper jsonMapper;
  private final EntityEventsRegistry entityEventsRegistry;

  @Override
  public Object deserializePayload(String payload,
                                   EventType eventType) throws EventPayloadDeserializationException {
    var eventPayloadClass = payloadClassForEventType(eventType);
    try {
      return jsonMapper.readValue(payload, eventPayloadClass);
    } catch (JacksonException jacksonException) {
      throw new EventPayloadDeserializationException(
          "Could not deserialize payload into class=%s".formatted(eventPayloadClass),
          jacksonException);
    }
  }

  private Class<?> payloadClassForEventType(EventType eventType) throws EventPayloadDeserializationException {
    return entityEventsRegistry.payloadClassFor(eventType)
        .orElseThrow(() -> new EventPayloadDeserializationException(
            "Could not find class for payload with %s".formatted(eventType)));
  }
}
