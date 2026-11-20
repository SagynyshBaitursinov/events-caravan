package dev.baitursinov.caravan.event.sourcing;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.NoPayload;
import dev.baitursinov.caravan.event.sourcing.applying.ApplyEvent;
import dev.baitursinov.caravan.event.sourcing.applying.EventApplyingException;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class EventSourcedEntityRecordEventTest {

  static final String CAR = "car";
  static final String TURNED_ON = "turned-on";
  static final String HONKED = "honked";

  record TurnedOnPayload(String reason) {
  }

  record UnrelatedPayload(int value) {
  }

  @EntityName(CAR)
  static class Car extends EventSourcedEntity {

    final String id;

    @Nullable
    String turnedOnReason;

    Car(String id) {
      this.id = id;
    }

    void turnOn(Object payload) {
      recordEvent(TURNED_ON, payload);
    }

    int honks;

    void honk() {
      recordEvent(HONKED);
    }

    @ApplyEvent(TURNED_ON)
    void applyTurnedOn(Event<TurnedOnPayload> event) {
      this.turnedOnReason = event.payload().reason();
    }

    @ApplyEvent(HONKED)
    void applyHonked(Event<NoPayload> event) {
      this.honks++;
    }

    @Override
    public String entityId() {
      return id;
    }
  }

  @Test
  void shouldRecordEventWithValidPayload() {
    var car = new Car("car-1");

    car.turnOn(new TurnedOnPayload("ignition key"));

    assertThat(car.turnedOnReason).isEqualTo("ignition key");
    assertThat(car.version()).isEqualTo(1L);
    assertThat(car.notProducedEvents())
        .singleElement()
        .satisfies(event -> {
          assertThat(event.eventName()).isEqualTo(TURNED_ON);
          assertThat(event.payload()).isEqualTo(new TurnedOnPayload("ignition key"));
        });
  }

  @Test
  void shouldRecordEventWithNoPayload() {
    var car = new Car("car-1");

    car.honk();

    assertThat(car.honks).isEqualTo(1);
    assertThat(car.version()).isEqualTo(1L);
    assertThat(car.notProducedEvents())
        .singleElement()
        .satisfies(event -> {
          assertThat(event.eventName()).isEqualTo(HONKED);
          assertThat(event.payload()).isSameAs(NoPayload.INSTANCE);
        });
  }

  @Test
  void shouldNotRecordEventWithInvalidPayload() {
    var car = new Car("car-1");

    assertThatThrownBy(() -> car.turnOn(new UnrelatedPayload(42)))
        .isInstanceOf(EventApplyingException.class)
        .hasCauseInstanceOf(ClassCastException.class);

    assertThat(car.turnedOnReason).isNull();
    assertThat(car.version()).isEqualTo(0L);
    assertThat(car.notProducedEvents()).isEmpty();
  }

  @Test
  void shouldKeepPreviouslyRecordedEventsWhenRecordingEventWithInvalidPayloadFails() {
    var car = new Car("car-1");
    car.turnOn(new TurnedOnPayload("ignition key"));

    assertThatThrownBy(() -> car.turnOn(new UnrelatedPayload(42)))
        .isInstanceOf(EventApplyingException.class);

    assertThat(car.turnedOnReason).isEqualTo("ignition key");
    assertThat(car.version()).isEqualTo(1L);
    assertThat(car.notProducedEvents())
        .singleElement()
        .satisfies(event -> assertThat(event.payload()).isEqualTo(new TurnedOnPayload("ignition key")));
  }
}
