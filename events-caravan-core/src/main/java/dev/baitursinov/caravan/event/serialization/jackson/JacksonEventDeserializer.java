package dev.baitursinov.caravan.event.serialization.jackson;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.EventType;
import dev.baitursinov.caravan.event.serialization.EventDeserializationException;
import dev.baitursinov.caravan.event.serialization.EventDeserializer;
import dev.baitursinov.caravan.event.serialization.EventPayloadDeserializationException;
import dev.baitursinov.caravan.event.serialization.EventPayloadDeserializer;
import lombok.RequiredArgsConstructor;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayOutputStream;
import java.time.ZonedDateTime;

import static java.nio.charset.StandardCharsets.UTF_8;
import static tools.jackson.core.JsonToken.PROPERTY_NAME;

@RequiredArgsConstructor
public class JacksonEventDeserializer implements EventDeserializer {

  private static final String ENTITY_REFERENCE = "entityReference";
  private static final String EVENT_NAME = "eventName";
  private static final String SEQUENCE_NUMBER = "sequenceNumber";
  private static final String TIMESTAMP = "timestamp";
  private static final String PAYLOAD = "payload";

  private final JsonMapper jsonMapper;
  private final EventPayloadDeserializer eventPayloadDeserializer;

  @Override
  public Event<?> deserialize(String eventMessage) throws EventDeserializationException {
    try {
      return deserializeUsingParser(eventMessage);
    } catch (EventDeserializationException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new EventDeserializationException(
          "Event message could not be deserialized",
          exception);
    }
  }

  private Event<Object> deserializeUsingParser(String eventMessage) throws EventDeserializationException, EventPayloadDeserializationException {
    try (var jsonParser = jsonMapper.createParser(eventMessage)) {
      EntityReference entityReference = null;
      String eventName = null;
      long sequenceNumber = 0;
      ZonedDateTime timestamp = null;
      String payloadAsJson = null;

      while (jsonParser.nextToken() != null) {
        if (jsonParser.currentToken() != PROPERTY_NAME) continue;
        String propertyName = jsonParser.currentName();
        jsonParser.nextToken();

        switch (propertyName) {
          case ENTITY_REFERENCE -> entityReference = jsonParser.readValueAs(EntityReference.class);
          case EVENT_NAME -> eventName = jsonParser.getValueAsString();
          case SEQUENCE_NUMBER -> sequenceNumber = jsonParser.getValueAsLong();
          case TIMESTAMP -> timestamp = ZonedDateTime.parse(jsonParser.getValueAsString());
          case PAYLOAD -> payloadAsJson = extractRawJson(jsonParser);
          default -> jsonParser.skipChildren();
        }
      }

      if (entityReference == null || eventName == null || sequenceNumber == 0 || timestamp == null) {
        throw new EventDeserializationException("Event payload does not contain necessary fields");
      }

      var eventType = new EventType(
          entityReference.entityName(),
          eventName);

      return Event.builder()
          .entityReference(entityReference)
          .eventName(eventName)
          .sequenceNumber(sequenceNumber)
          .timestamp(timestamp)
          .payload(eventPayloadDeserializer.deserializePayload(payloadAsJson, eventType))
          .build();
    }
  }

  private String extractRawJson(JsonParser jsonParser) {
    var out = new ByteArrayOutputStream(512);

    try (JsonGenerator generator = jsonMapper.createGenerator(out)) {
      generator.copyCurrentStructure(jsonParser);
    }

    return out.toString(UTF_8);
  }
}
