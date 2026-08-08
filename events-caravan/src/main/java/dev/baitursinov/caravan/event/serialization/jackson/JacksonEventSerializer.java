package dev.baitursinov.caravan.event.serialization.jackson;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.serialization.EventPayloadSerializationException;
import dev.baitursinov.caravan.event.serialization.EventSerializer;
import lombok.RequiredArgsConstructor;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@RequiredArgsConstructor
public class JacksonEventSerializer implements EventSerializer {

  private final JsonMapper jsonMapper;

  @Override
  public String serialize(Event<?> event) {
    try {
      return jsonMapper.writeValueAsString(event);
    } catch (JacksonException e) {
      throw new EventPayloadSerializationException(
          "Could not serialize %s"
              .formatted(event.eventReference()));
    }
  }
}
