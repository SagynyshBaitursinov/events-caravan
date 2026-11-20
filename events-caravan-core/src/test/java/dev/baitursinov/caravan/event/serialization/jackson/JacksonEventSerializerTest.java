package dev.baitursinov.caravan.event.serialization.jackson;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.NoPayload;
import dev.baitursinov.caravan.event.serialization.EventSerializationException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.ZonedDateTime;

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
  void shouldSerializeEvent() {
    var json = serializer.serialize(event(new CarTurnedOnPayload("red")));

    assertThat(json).isEqualTo(
        "{\"entityReference\":{\"entityName\":\"car\",\"entityId\":\"1\"},\"eventName\":\"turned-on\",\"sequenceNumber\":1,\"timestamp\":\"2026-01-01T10:15:30Z\",\"payload\":{\"color\":\"red\"}}");
  }

  @Test
  void shouldSerializeUnsequencedEvent() {
    var json = serializer.serialize(unsequencedEvent(new CarTurnedOnPayload("red")));

    assertThat(json).isEqualTo(
        "{\"entityReference\":{\"entityName\":\"car\",\"entityId\":\"1\"},\"eventName\":\"turned-on\",\"naturalKey\":\"a1b2\",\"timestamp\":\"2026-01-01T10:15:30Z\",\"payload\":{\"color\":\"red\"}}");
  }

  @Test
  void shouldSerializeUnsequencedEventFromStoredPartsIdenticallyToWholeEvent() {
    var event = unsequencedEvent(new CarTurnedOnPayload("red"));
    var serializedPayload = new JacksonEventPayloadSerializer(jsonMapper).serializePayload(event);

    var fromParts = serializer.serialize(event.eventReference(), event.timestamp(), serializedPayload);

    assertThat(fromParts).isEqualTo(serializer.serialize(event));
  }

  @Test
  void shouldThrowWhenPayloadCannotBeSerialized() {
    var event = event(new ExplodingPayload());

    assertThatThrownBy(() -> serializer.serialize(event))
        .isInstanceOf(EventSerializationException.class)
        .hasMessageContaining(event.eventReference().toString());
  }

  @Test
  void shouldSerializeFromStoredPartsIdenticallyToWholeEvent() {
    var event = event(new CarTurnedOnPayload("red"));
    var serializedPayload = new JacksonEventPayloadSerializer(jsonMapper).serializePayload(event);

    var fromParts = serializer.serialize(event.eventReference(), event.timestamp(), serializedPayload);

    assertThat(fromParts).isEqualTo(serializer.serialize(event));
  }

  @Test
  void shouldNormalizeTimestampOfStoredPartsLikeEvent() {
    var event = event(new CarTurnedOnPayload("red"));
    var serializedPayload = new JacksonEventPayloadSerializer(jsonMapper).serializePayload(event);

    var fromParts = serializer.serialize(
        event.eventReference(),
        ZonedDateTime.parse("2026-01-01T12:15:30.000999+02:00"),
        serializedPayload);

    assertThat(fromParts).isEqualTo(serializer.serialize(event));
  }

  @Test
  void shouldEmbedSerializedPayloadVerbatim() {
    var event = event(new CarTurnedOnPayload("red"));

    var fromParts = serializer.serialize(event.eventReference(), event.timestamp(), "{\"color\" :  \"red\"}");

    assertThat(fromParts).endsWith("\"payload\":{\"color\" :  \"red\"}}");
  }

  @Test
  void shouldOmitNoPayload() {
    var json = serializer.serialize(event(NoPayload.INSTANCE));

    assertThat(json).isEqualTo(
        "{\"entityReference\":{\"entityName\":\"car\",\"entityId\":\"1\"},\"eventName\":\"turned-on\",\"sequenceNumber\":1,\"timestamp\":\"2026-01-01T10:15:30Z\"}");
  }

  @Test
  void shouldSerializeEventWithNoPayloadFromStoredPartsIdenticallyToWholeEvent() {
    var event = event(NoPayload.INSTANCE);
    var serializedPayload = new JacksonEventPayloadSerializer(jsonMapper).serializePayload(event);

    var fromParts = serializer.serialize(event.eventReference(), event.timestamp(), serializedPayload);

    assertThat(fromParts).isEqualTo(serializer.serialize(event));
  }

  private Event<Object> unsequencedEvent(Object payload) {
    return Event.unsequenced()
        .entityReference(new EntityReference("car", "1"))
        .eventName("turned-on")
        .naturalKey("a1b2")
        .timestamp(ZonedDateTime.parse("2026-01-01T10:15:30Z"))
        .payload(payload)
        .build();
  }

  private Event<Object> event(Object payload) {
    return Event.sequenced()
        .entityReference(new EntityReference("car", "1"))
        .eventName("turned-on")
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.parse("2026-01-01T10:15:30Z"))
        .payload(payload)
        .build();
  }
}
