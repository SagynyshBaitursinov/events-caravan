package io.saga.caravan.event.serialization;

import io.saga.caravan.event.Event;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Component
@RequiredArgsConstructor
public class DefaultEventPayloadSerializer implements EventPayloadSerializer {

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
