package io.saga.caravan.event.serialization;

import io.saga.caravan.event.EventType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

@Component
@RequiredArgsConstructor
public class DefaultEventPayloadDeserializer implements EventPayloadDeserializer {

  private final JsonMapper jsonMapper;
  private final Map<EventType, Class<?>> eventPayloadClassMap;

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
    if (eventPayloadClassMap.containsKey(eventType)) {
      return eventPayloadClassMap.get(eventType);
    } else {
      throw new EventPayloadDeserializationException(
          "Could not find class for payload with %s".formatted(eventType));
    }
  }
}
