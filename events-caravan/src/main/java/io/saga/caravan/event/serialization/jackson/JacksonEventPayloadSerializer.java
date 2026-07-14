package io.saga.caravan.event.serialization.jackson;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.serialization.EventPayloadSerializationException;
import io.saga.caravan.event.serialization.EventPayloadSerializer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Component
@RequiredArgsConstructor
public class JacksonEventPayloadSerializer implements EventPayloadSerializer {

  private final JsonMapper jsonMapper;

  @Override
  public String serializePayload(Event<?> event) {
    try {
      return jsonMapper.writeValueAsString(event.payload());
    } catch (JacksonException e) {
      throw new EventPayloadSerializationException(
          "Could not serialize payload for %s"
              .formatted(event.eventReference()));
    }
  }
}
