package dev.baitursinov.caravan.event.serialization.jackson;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.serialization.EventPayloadSerializationException;
import dev.baitursinov.caravan.event.serialization.EventPayloadSerializer;
import lombok.RequiredArgsConstructor;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

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
