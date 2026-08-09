package dev.baitursinov.caravan.test.event.handler;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.consumer.handler.EventHandler;
import dev.baitursinov.caravan.test.value.NumberCarryingPayload;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class NumberCarryingEventsHandler implements EventHandler<NumberCarryingPayload> {

  @Getter
  private final List<Event<NumberCarryingPayload>> events = new ArrayList<>();

  @Override
  public boolean isOfInterest(Event<NumberCarryingPayload> event) {
    return true;
  }

  @Override
  public synchronized void handle(Event<NumberCarryingPayload> event) {
    events.add(event);
  }

  public synchronized List<Event<NumberCarryingPayload>> getEventsOfEntity(String entityId) {
    return events.stream()
        .filter(event -> event.entityReference().entityId().equals(entityId))
        .toList();
  }
}
