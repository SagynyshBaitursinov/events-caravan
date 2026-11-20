package dev.baitursinov.caravan.event.producer;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.EntityEventsRegistration;
import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.NoPayload;
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
class SequencedEventProducerTest {

  static class CarTurnedOnPayload {
  }

  static class SportsCarTurnedOnPayload extends CarTurnedOnPayload {
  }

  static class RecordingEventWriter implements SequencedEventWriter {

    final List<Event<?>> writtenEvents = new ArrayList<>();
    final List<Integer> writtenBatchSizes = new ArrayList<>();

    @Override
    public void write(ValidatedEvents events) {
      writtenBatchSizes.add(events.size());
      writtenEvents.addAll(events.events());
    }
  }

  RecordingEventWriter writer = new RecordingEventWriter();

  SequencedEventProducer eventProducer = new SequencedEventProducer(
      writer,
      EntityEventsRegistry.createFor(
          List.of(new EntityEventsRegistration("car", Map.of(
              "turned-on", CarTurnedOnPayload.class,
              "honked", NoPayload.class)))));

  @Test
  void shouldProduceEventWithRegisteredPayloadClass() {
    var event = event("turned-on", new CarTurnedOnPayload());

    assertThatCode(() -> eventProducer.produce(event))
        .doesNotThrowAnyException();

    assertThat(writer.writtenEvents).containsExactly(event);
  }

  @Test
  void shouldProduceEventCollectionWithRegisteredPayloadClass() {
    var event = event("turned-on", new CarTurnedOnPayload());

    assertThatCode(() -> eventProducer.produce(List.of(event)))
        .doesNotThrowAnyException();

    assertThat(writer.writtenEvents).containsExactly(event);
  }

  @Test
  void shouldThrowOnUnregisteredEventType() {
    var event = event("turned-off", new CarTurnedOnPayload());

    assertThatThrownBy(() -> eventProducer.produce(event))
        .isInstanceOf(EventProductionException.class)
        .hasMessageContaining("car")
        .hasMessageContaining("turned-off");

    assertThat(writer.writtenEvents).isEmpty();
  }

  @Test
  void shouldThrowOnPayloadClassNotMatchingRegisteredOne() {
    var event = event("turned-on", "not a car payload");

    assertThatThrownBy(() -> eventProducer.produce(event))
        .isInstanceOf(EventProductionException.class)
        .hasMessageContaining(String.class.getName())
        .hasMessageContaining(CarTurnedOnPayload.class.getName());

    assertThat(writer.writtenEvents).isEmpty();
  }

  @Test
  void shouldProduceEventWithNoPayload() {
    var event = event("honked", NoPayload.INSTANCE);

    assertThatCode(() -> eventProducer.produce(event))
        .doesNotThrowAnyException();

    assertThat(writer.writtenEvents).containsExactly(event);
  }

  @Test
  void shouldThrowOnPayloadClassBeingSubclassOfRegisteredOne() {
    var event = event("turned-on", new SportsCarTurnedOnPayload());

    assertThatThrownBy(() -> eventProducer.produce(event))
        .isInstanceOf(EventProductionException.class)
        .hasMessageContaining(SportsCarTurnedOnPayload.class.getName());

    assertThat(writer.writtenEvents).isEmpty();
  }

  @Test
  void shouldThrowOnEventCollectionContainingInvalidEvent() {
    var validEvent = event("turned-on", new CarTurnedOnPayload());
    var invalidEvent = event("turned-off", new CarTurnedOnPayload());

    assertThatThrownBy(() -> eventProducer.produce(List.of(validEvent, invalidEvent)))
        .isInstanceOf(EventProductionException.class);

    assertThat(writer.writtenEvents).isEmpty();
  }

  @Test
  void shouldWriteEventCollectionAsOneBatch() {
    var first = event("turned-on", new CarTurnedOnPayload());
    var second = event("turned-on", new CarTurnedOnPayload());

    eventProducer.produce(List.of(first, second));

    assertThat(writer.writtenEvents).containsExactly(first, second);
    assertThat(writer.writtenBatchSizes).containsExactly(2);
  }

  @Test
  void shouldRejectUnsequencedEventWithoutWritingAnything() {
    var sequenced = event("turned-on", new CarTurnedOnPayload());
    Event<Object> unsequenced = Event.unsequenced()
        .entityReference(new EntityReference("car", "1"))
        .eventName("turned-on")
        .naturalKey("a1b2")
        .timestamp(ZonedDateTime.now())
        .payload(new CarTurnedOnPayload())
        .build();

    assertThatThrownBy(() -> eventProducer.produce(List.of(sequenced, unsequenced)))
        .isInstanceOf(EventProductionException.class)
        .hasMessageContaining("has no sequenceNumber")
        .hasMessageContaining("UnsequencedEventProducer");

    assertThat(writer.writtenEvents).isEmpty();
  }

  @Test
  void shouldPropagateWriterFailureUnchanged() {
    var duplicate = new DuplicateEventProductionException("already exists");
    SequencedEventProducer failingProducer = new SequencedEventProducer(
        events -> {
          throw duplicate;
        },
        EntityEventsRegistry.createFor(
            List.of(new EntityEventsRegistration("car", Map.of("turned-on", CarTurnedOnPayload.class)))));

    assertThatThrownBy(() -> failingProducer.produce(event("turned-on", new CarTurnedOnPayload())))
        .isSameAs(duplicate);
  }

  private Event<Object> event(String eventName, Object payload) {
    return Event.sequenced()
        .entityReference(new EntityReference("car", "1"))
        .eventName(eventName)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(payload)
        .build();
  }
}
