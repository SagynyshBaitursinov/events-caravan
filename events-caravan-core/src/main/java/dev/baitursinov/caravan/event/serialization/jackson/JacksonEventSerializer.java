package dev.baitursinov.caravan.event.serialization.jackson;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.EventReference;
import dev.baitursinov.caravan.event.serialization.EventSerializationException;
import dev.baitursinov.caravan.event.serialization.EventSerializer;
import lombok.RequiredArgsConstructor;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.util.RawValue;

import java.time.ZonedDateTime;

import static dev.baitursinov.caravan.event.Event.normalize;

@RequiredArgsConstructor
public class JacksonEventSerializer implements EventSerializer {

  private final JsonMapper jsonMapper;

  @Override
  public String serialize(Event<?> event) {
    try {
      return jsonMapper.writeValueAsString(event);
    } catch (JacksonException e) {
      throw new EventSerializationException(
          "Could not serialize %s".formatted(event.eventReference()));
    }
  }

  @Override
  public String serialize(EventReference eventReference,
                          ZonedDateTime timestamp,
                          String serializedPayload) {
    try {
      var node = jsonMapper.createObjectNode();
      node.putPOJO("entityReference", eventReference.entityReference());
      node.put("eventName", eventReference.eventName());
      node.put("sequenceNumber", eventReference.sequenceNumber());
      node.putPOJO("timestamp", normalize(timestamp));
      node.putRawValue("payload", new RawValue(serializedPayload));
      return jsonMapper.writeValueAsString(node);
    } catch (JacksonException exception) {
      throw new EventSerializationException(
          "Could not serialize %s".formatted(eventReference));
    }
  }
}
