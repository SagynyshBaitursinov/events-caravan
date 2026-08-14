package dev.baitursinov.caravan.event.sourcing.snapshot.jackson;

import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class JacksonSnapshotDeserializerTest {

  record CounterSnapshot(int value) {
  }

  JsonMapper jsonMapper = JsonMapper.builder().build();

  JacksonSnapshotDeserializer deserializer = new JacksonSnapshotDeserializer(jsonMapper);

  @Test
  void shouldDeserializeSnapshotPayload() {
    var payload = deserializer.deserializePayload("{\"value\": 42}", CounterSnapshot.class);

    assertThat(payload).isEqualTo(new CounterSnapshot(42));
  }

  @Test
  void shouldThrowOnMalformedPayload() {
    assertThatThrownBy(() -> deserializer.deserializePayload("{not valid", CounterSnapshot.class))
        .isInstanceOf(SnapshotException.class)
        .hasMessageContaining(CounterSnapshot.class.getName());
  }
}
