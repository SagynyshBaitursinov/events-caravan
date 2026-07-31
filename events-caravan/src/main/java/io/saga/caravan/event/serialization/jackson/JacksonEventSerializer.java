package io.saga.caravan.event.serialization.jackson;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.serialization.EventPayloadSerializationException;
import io.saga.caravan.event.serialization.EventSerializer;
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
