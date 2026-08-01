package io.saga.caravan.event.serialization.jackson;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.Event;
import io.saga.caravan.event.serialization.EventPayloadSerializationException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.ZonedDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class JacksonEventSerializerTest {

  record CarTurnedOnPayload(String color) {
  }

  static class ExplodingPayload {

    @SuppressWarnings("unused")
    public String getValue() {
      throw new RuntimeException("boom");
    }
  }

  JsonMapper jsonMapper = JsonMapper.builder().build();

  JacksonEventSerializer serializer = new JacksonEventSerializer(jsonMapper);

  @Test
  void shouldSerializeEventToJson() {
    var event = event(new CarTurnedOnPayload("red"));

    var json = serializer.serialize(event);

    @SuppressWarnings("unchecked")
    var asMap = (Map<String, Object>) jsonMapper.readValue(json, Map.class);

    @SuppressWarnings("unchecked")
    var entityReference = (Map<String, Object>) asMap.get("entityReference");
    assertThat(entityReference)
        .containsEntry("entityName", "car")
        .containsEntry("entityId", "1");
    assertThat(asMap.get("eventName")).isEqualTo("turned-on");
    assertThat(((Number) asMap.get("sequenceNumber")).longValue()).isEqualTo(1L);

    @SuppressWarnings("unchecked")
    var payload = (Map<String, Object>) asMap.get("payload");
    assertThat(payload).containsEntry("color", "red");
  }

  @Test
  void shouldThrowWhenPayloadCannotBeSerialized() {
    var event = event(new ExplodingPayload());

    assertThatThrownBy(() -> serializer.serialize(event))
        .isInstanceOf(EventPayloadSerializationException.class)
        .hasMessageContaining(event.eventReference().toString());
  }

  private Event<Object> event(Object payload) {
    return Event.builder()
        .entityReference(new EntityReference("car", "1"))
        .eventName("turned-on")
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.parse("2026-01-01T10:15:30Z"))
        .payload(payload)
        .build();
  }
}
