package dev.baitursinov.caravan.event;

import dev.baitursinov.caravan.entity.EntityReference;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class EventTest {

  static final EntityReference CAR = new EntityReference("car", "1");
  static final ZonedDateTime TIMESTAMP = ZonedDateTime.parse("2026-01-01T10:15:30Z");

  @Test
  void shouldBuildSequencedEvent() {
    var event = Event.sequenced()
        .entityReference(CAR)
        .eventName("turned-on")
        .sequenceNumber(3)
        .timestamp(TIMESTAMP)
        .payload("payload")
        .build();

    assertThat(event.eventReference()).isEqualTo(new SequencedEventReference(CAR, 3, "turned-on"));
    assertThat(event.entityReference()).isEqualTo(CAR);
    assertThat(event.eventName()).isEqualTo("turned-on");
  }

  @Test
  void shouldBuildUnsequencedEvent() {
    var event = Event.unsequenced()
        .entityReference(CAR)
        .eventName("turned-on")
        .naturalKey("a1b2")
        .timestamp(TIMESTAMP)
        .payload("payload")
        .build();

    assertThat(event.eventReference()).isEqualTo(new UnsequencedEventReference(CAR, "a1b2", "turned-on"));
  }

  @Test
  void shouldRejectSequencedEventWithoutSequenceNumber() {
    var builder = Event.sequenced()
        .entityReference(CAR)
        .eventName("turned-on")
        .timestamp(TIMESTAMP)
        .payload("payload");

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sequenceNumber");
  }

  @Test
  void shouldRejectUnsequencedEventWithoutNaturalKey() {
    var builder = Event.unsequenced()
        .entityReference(CAR)
        .eventName("turned-on")
        .timestamp(TIMESTAMP)
        .payload("payload");

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("naturalKey");
  }

  @Test
  void shouldRejectBlankNaturalKey() {
    var builder = Event.unsequenced()
        .entityReference(CAR)
        .eventName("turned-on")
        .naturalKey(" ")
        .timestamp(TIMESTAMP)
        .payload("payload");

    assertThatThrownBy(builder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("naturalKey");
  }

  @Test
  void shouldBeEqualWhenEventReferencesAreEqual() {
    var first = Event.unsequenced()
        .entityReference(CAR).eventName("turned-on").naturalKey("a1b2")
        .timestamp(TIMESTAMP).payload("first")
        .build();
    var duplicate = Event.unsequenced()
        .entityReference(CAR).eventName("turned-on").naturalKey("a1b2")
        .timestamp(TIMESTAMP.plusSeconds(5)).payload("second")
        .build();
    var other = Event.unsequenced()
        .entityReference(CAR).eventName("turned-on").naturalKey("c3d4")
        .timestamp(TIMESTAMP).payload("first")
        .build();

    assertThat(first).isEqualTo(duplicate).hasSameHashCodeAs(duplicate);
    assertThat(first).isNotEqualTo(other);
  }
}
