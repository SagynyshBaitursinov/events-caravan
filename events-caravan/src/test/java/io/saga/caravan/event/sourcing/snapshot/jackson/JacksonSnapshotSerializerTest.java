package io.saga.caravan.event.sourcing.snapshot.jackson;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.sourcing.snapshot.EntitySnapshot;
import io.saga.caravan.event.sourcing.snapshot.SnapshotException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class JacksonSnapshotSerializerTest {

  record CounterSnapshot(int value) {
  }

  static class ExplodingPayload {

    @SuppressWarnings("unused")
    public String getValue() {
      throw new RuntimeException("boom");
    }
  }

  JsonMapper jsonMapper = JsonMapper.builder().build();

  JacksonSnapshotSerializer serializer = new JacksonSnapshotSerializer(jsonMapper);

  @Test
  void shouldSerializeSnapshotPayload() {
    var snapshot = snapshot(new CounterSnapshot(42));

    var json = serializer.serializePayload(snapshot);

    assertThat(jsonMapper.readValue(json, CounterSnapshot.class))
        .isEqualTo(new CounterSnapshot(42));
  }

  @Test
  void shouldThrowWhenPayloadCannotBeSerialized() {
    var snapshot = snapshot(new ExplodingPayload());

    assertThatThrownBy(() -> serializer.serializePayload(snapshot))
        .isInstanceOf(SnapshotException.class)
        .hasMessageContaining(snapshot.entityReference().toString())
        .hasMessageContaining(String.valueOf(snapshot.version()));
  }

  private EntitySnapshot<Object> snapshot(Object payload) {
    return EntitySnapshot.builder()
        .entityReference(new EntityReference("counter", "1"))
        .version(3)
        .payload(payload)
        .build();
  }
}
