package io.saga.caravan.event.sourcing;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.Event;
import io.saga.caravan.event.sourcing.applying.ApplyEvent;
import io.saga.caravan.event.sourcing.applying.EventApplyingException;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class EntityEventApplierTest {

  static final String CAR = "car";
  static final String TURNED_ON = "turned-on";

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

    @ApplyEvent(TURNED_ON)
    void applyTurnedOn(Event<TurnedOnPayload> event) {
      this.turnedOnReason = event.payload().reason();
    }

    @Override
    public String entityId() {
      return id;
    }
  }

  private <T> Event<T> eventOf(String eventName, T payload) {
    return Event.<T>builder()
        .entityReference(new EntityReference(CAR, "car-1"))
        .sequenceNumber(1L)
        .eventName(eventName)
        .timestamp(ZonedDateTime.now())
        .payload(payload)
        .build();
  }

  @Test
  void shouldThrowWhenAppliedEventPayloadDoesNotMatchApplyMethodsExpectedType() {
    var car = new Car("car-1");
    var eventWithWrongPayload = eventOf(TURNED_ON, new UnrelatedPayload(42));

    assertThatThrownBy(() -> EntityEventApplier.apply(car, eventWithWrongPayload))
        .isInstanceOf(EventApplyingException.class)
        .hasMessage("Cannot invoke apply method io.saga.caravan.event.sourcing.EntityEventApplierTest$Car.applyTurnedOn with parameter Event{Entity{car:car-1}:1(turned-on)} of payload type class io.saga.caravan.event.sourcing.EntityEventApplierTest$UnrelatedPayload")
        .hasCauseInstanceOf(ClassCastException.class);

    assertThat(car.turnedOnReason).isNull();
    assertThat(car.version()).isEqualTo(0L);
  }

  @Test
  void shouldThrowWhenNoApplyMethodExistsForEventName() {
    var car = new Car("car-1");
    var event = eventOf("unknown-event", new TurnedOnPayload("ignition"));

    assertThatThrownBy(() -> EntityEventApplier.apply(car, event))
        .isInstanceOf(EventApplyingException.class)
        .hasMessage("No @ApplyEvent method for eventName=unknown-event in entity class io.saga.caravan.event.sourcing.EntityEventApplierTest$Car");

    assertThat(car.turnedOnReason).isNull();
    assertThat(car.version()).isEqualTo(0L);
  }

  @Test
  void shouldApplyMatchingEventAndIncrementVersion() {
    var car = new Car("car-1");
    var event = eventOf(TURNED_ON, new TurnedOnPayload("ignition key"));

    EntityEventApplier.apply(car, event);

    assertThat(car.turnedOnReason).isEqualTo("ignition key");
    assertThat(car.version()).isEqualTo(1L);
  }
}
