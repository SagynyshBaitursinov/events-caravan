package io.saga.caravan.event.serialization.jackson;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.serialization.EventDeserializationException;
import io.saga.caravan.event.serialization.EventPayloadDeserializationException;
import io.saga.caravan.event.serialization.EventPayloadDeserializer;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class JacksonEventDeserializerTest {

  record CarTurnedOnPayload(String color) {
  }

  static class StubEventPayloadDeserializer implements EventPayloadDeserializer {

    @Nullable Object payloadToReturn;
    @Nullable EventPayloadDeserializationException exceptionToThrow;
    @Nullable String capturedPayload;
    @Nullable EventType capturedEventType;

    @SuppressWarnings("DataFlowIssue")
    @Override
    public Object deserializePayload(String payload, EventType eventType) throws EventPayloadDeserializationException {
      this.capturedPayload = payload;
      this.capturedEventType = eventType;
      if (exceptionToThrow != null) throw exceptionToThrow;
      return payloadToReturn;
    }
  }

  JsonMapper jsonMapper = JsonMapper.builder().build();
  StubEventPayloadDeserializer payloadDeserializer = new StubEventPayloadDeserializer();
  JacksonEventDeserializer deserializer = new JacksonEventDeserializer(jsonMapper, payloadDeserializer);

  @Test
  void shouldDeserializeEventFromJson() throws Exception {
    payloadDeserializer.payloadToReturn = new CarTurnedOnPayload("red");

    var json = """
        {
          "entityReference": {"entityName": "car", "entityId": "1"},
          "eventName": "turned-on",
          "sequenceNumber": 1,
          "timestamp": "2026-01-01T10:15:30Z",
          "payload": {"color": "red"}
        }
        """;

    var event = deserializer.deserialize(json);

    assertThat(event.entityReference()).isEqualTo(new EntityReference("car", "1"));
    assertThat(event.eventName()).isEqualTo("turned-on");
    assertThat(event.sequenceNumber()).isEqualTo(1L);
    assertThat(event.payload()).isEqualTo(new CarTurnedOnPayload("red"));
    assertThat(payloadDeserializer.capturedEventType).isEqualTo(new EventType("car", "turned-on"));
    assertThat(payloadDeserializer.capturedPayload).isEqualToIgnoringWhitespace("{\"color\": \"red\"}");
  }

  @Test
  void shouldThrowWhenRequiredFieldIsMissing() {
    var json = """
        {
          "eventName": "turned-on",
          "sequenceNumber": 1,
          "timestamp": "2026-01-01T10:15:30Z",
          "payload": {"color": "red"}
        }
        """;

    assertThatThrownBy(() -> deserializer.deserialize(json))
        .isInstanceOf(EventDeserializationException.class)
        .hasMessageContaining("necessary fields");
  }

  @Test
  void shouldThrowOnMalformedJson() {
    assertThatThrownBy(() -> deserializer.deserialize("{not valid json"))
        .isInstanceOf(EventDeserializationException.class);
  }

  @Test
  void shouldWrapPayloadDeserializationException() {
    payloadDeserializer.exceptionToThrow = new EventPayloadDeserializationException("boom");

    var json = """
        {
          "entityReference": {"entityName": "car", "entityId": "1"},
          "eventName": "turned-on",
          "sequenceNumber": 1,
          "timestamp": "2026-01-01T10:15:30Z",
          "payload": {"color": "red"}
        }
        """;

    assertThatThrownBy(() -> deserializer.deserialize(json))
        .isInstanceOf(EventDeserializationException.class)
        .hasCauseInstanceOf(EventPayloadDeserializationException.class);
  }
}
