package dev.baitursinov.caravan.event.consumer.handler;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.NoPayload;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class HandlerBasedEventConsumerTest {

  static class CarTurnedOnPayload {
  }

  static class SportsCarTurnedOnPayload extends CarTurnedOnPayload {
  }

  static class TruckTurnedOnPayload {
  }

  static class RecordingEventHandler implements EventHandler<CarTurnedOnPayload> {

    final List<Event<CarTurnedOnPayload>> handledEvents = new ArrayList<>();
    boolean interested = true;

    @Override
    public boolean isOfInterest(Event<CarTurnedOnPayload> event) {
      return interested;
    }

    @Override
    public void handle(Event<CarTurnedOnPayload> event) {
      handledEvents.add(event);
    }
  }

  static class RecordingNoPayloadEventHandler implements EventHandler<NoPayload> {

    final List<Event<NoPayload>> handledEvents = new ArrayList<>();

    @Override
    public boolean isOfInterest(Event<NoPayload> event) {
      return true;
    }

    @Override
    public void handle(Event<NoPayload> event) {
      handledEvents.add(event);
    }
  }

  static class RecordingCatchAllEventHandler implements EventHandler<Object> {

    final List<Event<Object>> handledEvents = new ArrayList<>();

    @Override
    public boolean isOfInterest(Event<Object> event) {
      return true;
    }

    @Override
    public void handle(Event<Object> event) {
      handledEvents.add(event);
    }
  }

  @SuppressWarnings("rawtypes")
  static class MisconfiguredHandler implements EventHandler {

    @Override
    public boolean isOfInterest(Event event) {
      return true;
    }

    @Override
    public void handle(Event event) {
    }
  }

  @Test
  void shouldInvokeHandlerInterestedInMatchingPayload() {
    var handler = new RecordingEventHandler();
    var consumer = new HandlerBasedEventConsumer(List.of(handler));
    var event = event(new CarTurnedOnPayload());

    consumer.consume(event);

    assertThat(handler.handledEvents).containsExactly(event);
  }

  @Test
  void shouldInvokeHandlerForPayloadSubclass() {
    var handler = new RecordingEventHandler();
    var consumer = new HandlerBasedEventConsumer(List.of(handler));
    var event = this.<CarTurnedOnPayload>event(new SportsCarTurnedOnPayload());

    consumer.consume(event);

    assertThat(handler.handledEvents).containsExactly(event);
  }

  @Test
  void shouldNotInvokeHandlerForNonMatchingPayloadType() {
    var handler = new RecordingEventHandler();
    var consumer = new HandlerBasedEventConsumer(List.of(handler));
    var event = event(new TruckTurnedOnPayload());

    consumer.consume(event);

    assertThat(handler.handledEvents).isEmpty();
  }

  @Test
  void shouldNotInvokeHandlerNotInterestedInEvent() {
    var handler = new RecordingEventHandler();
    handler.interested = false;
    var consumer = new HandlerBasedEventConsumer(List.of(handler));
    var event = event(new CarTurnedOnPayload());

    consumer.consume(event);

    assertThat(handler.handledEvents).isEmpty();
  }

  @Test
  void shouldInvokeOnlyHandlersInterestedInGivenEvent() {
    var carHandler = new RecordingEventHandler();
    var uninterestedCarHandler = new RecordingEventHandler();
    uninterestedCarHandler.interested = false;
    var consumer = new HandlerBasedEventConsumer(List.of(carHandler, uninterestedCarHandler));
    var event = event(new CarTurnedOnPayload());

    consumer.consume(event);

    assertThat(carHandler.handledEvents).containsExactly(event);
    assertThat(uninterestedCarHandler.handledEvents).isEmpty();
  }

  @Test
  void shouldInvokeNoPayloadAndCatchAllHandlersForEventWithNoPayload() {
    var carHandler = new RecordingEventHandler();
    var noPayloadHandler = new RecordingNoPayloadEventHandler();
    var catchAllHandler = new RecordingCatchAllEventHandler();
    var consumer = new HandlerBasedEventConsumer(List.of(carHandler, noPayloadHandler, catchAllHandler));
    var event = event(NoPayload.INSTANCE);

    consumer.consume(event);

    assertThat(noPayloadHandler.handledEvents).containsExactly(event);
    assertThat(catchAllHandler.handledEvents).hasSize(1);
    assertThat(carHandler.handledEvents).isEmpty();
  }

  @Test
  void shouldNotInvokeNoPayloadHandlerForEventWithPayload() {
    var noPayloadHandler = new RecordingNoPayloadEventHandler();
    var consumer = new HandlerBasedEventConsumer(List.of(noPayloadHandler));

    consumer.consume(event(new CarTurnedOnPayload()));

    assertThat(noPayloadHandler.handledEvents).isEmpty();
  }

  @SuppressWarnings("unchecked")
  @Test
  void shouldFailAtConstructionRatherThanOnFirstConsumedEventForMisconfiguredHandler() {
    assertThatThrownBy(() -> new HandlerBasedEventConsumer(List.of(new MisconfiguredHandler())))
        .isInstanceOf(EventHandlerSetupException.class);
  }

  private <T> Event<T> event(T payload) {
    return Event.<T>sequenced()
        .entityReference(new EntityReference("car", "1"))
        .eventName("turned-on")
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(payload)
        .build();
  }
}
