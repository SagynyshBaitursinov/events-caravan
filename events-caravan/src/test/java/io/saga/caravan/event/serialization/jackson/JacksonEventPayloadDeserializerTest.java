package io.saga.caravan.event.serialization.jackson;

import io.saga.caravan.event.EventPayloadClassMappingKeeper;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.serialization.EventPayloadDeserializationException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class JacksonEventPayloadDeserializerTest {

  record CarTurnedOnPayload(String color) {
  }

  JsonMapper jsonMapper = JsonMapper.builder().build();

  EventPayloadClassMappingKeeper mappingKeeper = new EventPayloadClassMappingKeeper()
      .register(new EventType("car", "turned-on"), CarTurnedOnPayload.class);

  JacksonEventPayloadDeserializer deserializer =
      new JacksonEventPayloadDeserializer(jsonMapper, mappingKeeper);

  @Test
  void shouldDeserializePayloadIntoRegisteredClass() throws Exception {
    var payload = deserializer.deserializePayload(
        "{\"color\": \"red\"}",
        new EventType("car", "turned-on"));

    assertThat(payload).isEqualTo(new CarTurnedOnPayload("red"));
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
