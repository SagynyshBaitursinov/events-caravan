package io.saga.caravan.event.producer;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.EntityEventsRegistration;
import io.saga.caravan.event.EntityEventsRegistry;
import io.saga.caravan.event.Event;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class ValidatingEventProducerTest {

  static class CarTurnedOnPayload {
  }

  static class SportsCarTurnedOnPayload extends CarTurnedOnPayload {
  }

  static class RecordingEventProducer implements EventProducer {

    final List<Event<?>> producedEvents = new ArrayList<>();

    @Override
    public void produce(Event<?> event) {
      producedEvents.add(event);
    }

    @Override
    public void produce(List<Event<?>> events) {
      producedEvents.addAll(events);
    }
  }

  RecordingEventProducer delegate = new RecordingEventProducer();

  ValidatingEventProducer validatingEventProducer = new ValidatingEventProducer(
      delegate,
      EntityEventsRegistry.createFor(
          List.of(new EntityEventsRegistration("car", Map.of("turned-on", CarTurnedOnPayload.class), true))));

  @Test
  void shouldProduceEventWithRegisteredPayloadClass() {
    var event = event("turned-on", new CarTurnedOnPayload());

    assertThatCode(() -> validatingEventProducer.produce(event))
        .doesNotThrowAnyException();

    assertThat(delegate.producedEvents).containsExactly(event);
  }

  @Test
  void shouldProduceEventCollectionWithRegisteredPayloadClass() {
    var event = event("turned-on", new CarTurnedOnPayload());

    assertThatCode(() -> validatingEventProducer.produce(List.of(event)))
        .doesNotThrowAnyException();

    assertThat(delegate.producedEvents).containsExactly(event);
  }

  @Test
  void shouldThrowOnUnregisteredEventType() {
    var event = event("turned-off", new CarTurnedOnPayload());

    assertThatThrownBy(() -> validatingEventProducer.produce(event))
        .isInstanceOf(EventProductionException.class)
        .hasMessageContaining("car")
        .hasMessageContaining("turned-off");

    assertThat(delegate.producedEvents).isEmpty();
  }

  @Test
  void shouldThrowOnPayloadClassNotMatchingRegisteredOne() {
    var event = event("turned-on", "not a car payload");

    assertThatThrownBy(() -> validatingEventProducer.produce(event))
        .isInstanceOf(EventProductionException.class)
        .hasMessageContaining(String.class.getName())
        .hasMessageContaining(CarTurnedOnPayload.class.getName());

    assertThat(delegate.producedEvents).isEmpty();
  }

  @Test
  void shouldThrowOnPayloadClassBeingSubclassOfRegisteredOne() {
    var event = event("turned-on", new SportsCarTurnedOnPayload());

    assertThatThrownBy(() -> validatingEventProducer.produce(event))
        .isInstanceOf(EventProductionException.class)
        .hasMessageContaining(SportsCarTurnedOnPayload.class.getName());

    assertThat(delegate.producedEvents).isEmpty();
  }

  @Test
  void shouldThrowOnEventCollectionContainingInvalidEvent() {
    var validEvent = event("turned-on", new CarTurnedOnPayload());
    var invalidEvent = event("turned-off", new CarTurnedOnPayload());

    assertThatThrownBy(() -> validatingEventProducer.produce(List.of(validEvent, invalidEvent)))
        .isInstanceOf(EventProductionException.class);

    assertThat(delegate.producedEvents).isEmpty();
  }

  private Event<Object> event(String eventName, Object payload) {
    return Event.builder()
        .entityReference(new EntityReference("car", "1"))
        .eventName(eventName)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(payload)
        .build();
  }
}
