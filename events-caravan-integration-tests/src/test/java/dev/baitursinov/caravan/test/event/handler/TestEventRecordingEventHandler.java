package dev.baitursinov.caravan.test.event.handler;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.consumer.handler.EventHandler;
import dev.baitursinov.caravan.test.value.TestEventPayload;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Records every {@link TestEventPayload} event it receives, whole and including repeated
 * deliveries, so tests can assert on what arrived rather than only on its payload.
 */
@Component
public class TestEventRecordingEventHandler implements EventHandler<TestEventPayload> {

  private final Queue<Event<TestEventPayload>> receivedEvents = new ConcurrentLinkedQueue<>();

  @Override
  public boolean isOfInterest(Event<TestEventPayload> event) {
    return true;
  }

  @Override
  public void handle(Event<TestEventPayload> event) {
    receivedEvents.add(event);
  }

  public List<Event<TestEventPayload>> receivedEventsWithTestField(String testField) {
    return receivedEvents.stream()
        .filter(event -> event.payload().testField().equals(testField))
        .toList();
  }
}
