package dev.baitursinov.caravan.test.event.handler;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.NoPayload;
import dev.baitursinov.caravan.event.consumer.handler.EventHandler;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

import static dev.baitursinov.caravan.test.event.registration.CalculatorEventsConfiguration.CALCULATOR;
import static dev.baitursinov.caravan.test.event.registration.CalculatorEventsConfiguration.RESET;

@Component
public class ResetEventsHandler implements EventHandler<NoPayload> {

  private final List<Event<NoPayload>> events = new ArrayList<>();

  @Override
  public boolean isOfInterest(Event<NoPayload> event) {
    return event.entityReference().entityName().equals(CALCULATOR)
        && event.eventName().equals(RESET);
  }

  @Override
  public synchronized void handle(Event<NoPayload> event) {
    events.add(event);
  }

  public synchronized List<Event<NoPayload>> getEventsOfEntity(String entityId) {
    return events.stream()
        .filter(event -> event.entityReference().entityId().equals(entityId))
        .toList();
  }
}
