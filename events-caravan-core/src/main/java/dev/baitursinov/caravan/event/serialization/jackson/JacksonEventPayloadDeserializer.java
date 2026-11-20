package dev.baitursinov.caravan.event.serialization.jackson;

import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.EventType;
import dev.baitursinov.caravan.event.NoPayload;
import dev.baitursinov.caravan.event.serialization.EventPayloadDeserializationException;
import dev.baitursinov.caravan.event.serialization.EventPayloadDeserializer;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@RequiredArgsConstructor
public class JacksonEventPayloadDeserializer implements EventPayloadDeserializer {

  private final JsonMapper jsonMapper;
  private final EntityEventsRegistry entityEventsRegistry;

  @Override
  public Object deserializePayload(@Nullable String payload,
                                   EventType eventType) throws EventPayloadDeserializationException {
    var eventPayloadClass = payloadClassForEventType(eventType);

    if (eventPayloadClass == NoPayload.class) {
      if (payload != null) {
        throw new EventPayloadDeserializationException(
            "Payload of %s must be absent, as its payload class is NoPayload".formatted(eventType));
      }
      return NoPayload.INSTANCE;
    }

    if (payload == null) {
      throw new EventPayloadDeserializationException(
          "Payload of %s is missing".formatted(eventType));
    }

    try {
      Object result = jsonMapper.readValue(payload, eventPayloadClass);
      if (result == null) {
        throw new EventPayloadDeserializationException(
            "Payload of %s must not be null".formatted(eventType));
      }
      return result;
    } catch (JacksonException jacksonException) {
      throw new EventPayloadDeserializationException(
          "Could not deserialize payload into class=%s".formatted(eventPayloadClass));
    }
  }

  private Class<?> payloadClassForEventType(EventType eventType) throws EventPayloadDeserializationException {
    return entityEventsRegistry.payloadClassFor(eventType)
        .orElseThrow(() -> new EventPayloadDeserializationException(
            "Could not find class for payload with %s".formatted(eventType)));
  }
}
