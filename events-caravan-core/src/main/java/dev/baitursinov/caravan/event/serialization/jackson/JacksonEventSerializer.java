package dev.baitursinov.caravan.event.serialization.jackson;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.EventReference;
import dev.baitursinov.caravan.event.NoPayload;
import dev.baitursinov.caravan.event.SequencedEventReference;
import dev.baitursinov.caravan.event.UnsequencedEventReference;
import dev.baitursinov.caravan.event.serialization.EventSerializationException;
import dev.baitursinov.caravan.event.serialization.EventSerializer;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.util.RawValue;

import java.time.ZonedDateTime;

import static dev.baitursinov.caravan.event.Event.normalize;

/**
 * Serializes events into JSON objects of {@code entityReference}, {@code eventName}, either
 * {@code sequenceNumber} (sequenced events) or {@code naturalKey} (unsequenced ones),
 * {@code timestamp} and {@code payload}. {@code payload} is omitted for events with
 * {@link NoPayload}.
 */
@RequiredArgsConstructor
public class JacksonEventSerializer implements EventSerializer {

  private final JsonMapper jsonMapper;

  @Override
  public String serialize(Event<?> event) {
    try {
      var node = metadataNode(event.eventReference(), event.timestamp());
      if (!(event.payload() instanceof NoPayload)) {
        node.putPOJO("payload", event.payload());
      }
      return jsonMapper.writeValueAsString(node);
    } catch (JacksonException e) {
      throw new EventSerializationException(
          "Could not serialize %s".formatted(event.eventReference()));
    }
  }

  @Override
  public String serialize(EventReference eventReference,
                          ZonedDateTime timestamp,
                          @Nullable String serializedPayload) {
    try {
      var node = metadataNode(eventReference, timestamp);
      if (serializedPayload != null) {
        node.putRawValue("payload", new RawValue(serializedPayload));
      }
      return jsonMapper.writeValueAsString(node);
    } catch (JacksonException exception) {
      throw new EventSerializationException(
          "Could not serialize %s".formatted(eventReference));
    }
  }

  private ObjectNode metadataNode(EventReference eventReference, ZonedDateTime timestamp) {
    var node = jsonMapper.createObjectNode();
    node.putPOJO("entityReference", eventReference.entityReference());
    node.put("eventName", eventReference.eventName());
    switch (eventReference) {
      case SequencedEventReference sequenced -> node.put("sequenceNumber", sequenced.sequenceNumber());
      case UnsequencedEventReference unsequenced -> node.put("naturalKey", unsequenced.naturalKey());
    }
    node.putPOJO("timestamp", normalize(timestamp));
    return node;
  }
}
