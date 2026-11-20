package dev.baitursinov.caravan.event.serialization.jackson;

import dev.baitursinov.caravan.event.EntityEventsRegistration;
import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.EventType;
import dev.baitursinov.caravan.event.NoPayload;
import dev.baitursinov.caravan.event.serialization.EventPayloadDeserializationException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class JacksonEventPayloadDeserializerTest {

  record CarTurnedOnPayload(String color) {
  }

  JsonMapper jsonMapper = JsonMapper.builder().build();

  EntityEventsRegistry entityEventsRegistry = EntityEventsRegistry.createFor(
      List.of(new EntityEventsRegistration("car", Map.of(
          "turned-on", CarTurnedOnPayload.class,
          "honked", NoPayload.class))));

  JacksonEventPayloadDeserializer deserializer =
      new JacksonEventPayloadDeserializer(jsonMapper, entityEventsRegistry);

  @Test
  void shouldDeserializePayloadIntoRegisteredClass() throws Exception {
    var payload = deserializer.deserializePayload(
        "{\"color\": \"red\"}",
        new EventType("car", "turned-on"));

    assertThat(payload).isEqualTo(new CarTurnedOnPayload("red"));
  }

  @Test
  void shouldDeserializeAbsentPayloadIntoNoPayload() throws Exception {
    var payload = deserializer.deserializePayload(null, new EventType("car", "honked"));

    assertThat(payload).isSameAs(NoPayload.INSTANCE);
  }

  @Test
  void shouldThrowWhenNoPayloadEventHasPayload() {
    assertThatThrownBy(() -> deserializer.deserializePayload(
        "{\"color\": \"red\"}",
        new EventType("car", "honked")))
        .isInstanceOf(EventPayloadDeserializationException.class)
        .hasMessage("Payload of EventType[entityName=car, eventName=honked] must be absent, as its payload class is NoPayload");
  }

  @Test
  void shouldThrowWhenPayloadIsAbsent() {
    assertThatThrownBy(() -> deserializer.deserializePayload(
        null,
        new EventType("car", "turned-on")))
        .isInstanceOf(EventPayloadDeserializationException.class)
        .hasMessage("Payload of EventType[entityName=car, eventName=turned-on] is missing");
  }

  @Test
  void shouldThrowWhenPayloadIsJsonNull() {
    assertThatThrownBy(() -> deserializer.deserializePayload(
        "null",
        new EventType("car", "turned-on")))
        .isInstanceOf(EventPayloadDeserializationException.class)
        .hasMessage("Payload of EventType[entityName=car, eventName=turned-on] must not be null");
  }

  @Test
  void shouldThrowOnUnregisteredEventType() {
    assertThatThrownBy(() -> deserializer.deserializePayload(
        "{\"color\": \"red\"}",
        new EventType("car", "turned-off")))
        .isInstanceOf(EventPayloadDeserializationException.class)
        .hasMessageContaining("turned-off");
  }

  @Test
  void shouldThrowOnMalformedPayload() {
    assertThatThrownBy(() -> deserializer.deserializePayload(
        "{not valid",
        new EventType("car", "turned-on")))
        .isInstanceOf(EventPayloadDeserializationException.class)
        .hasMessageContaining(CarTurnedOnPayload.class.getName());
  }
}
