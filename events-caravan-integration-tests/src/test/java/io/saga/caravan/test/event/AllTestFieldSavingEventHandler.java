package io.saga.caravan.test.event;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.consumer.handler.EventHandler;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

@Component
@Getter
public class AllTestFieldSavingEventHandler implements EventHandler<TestFieldContainingEventPayload> {

  private final Set<String> savedFields = new HashSet<>();

  @Override
  public boolean isOfInterest(Event<TestFieldContainingEventPayload> event) {
    return true;
  }

  @Override
  public void handle(Event<TestFieldContainingEventPayload> event) {
    savedFields.add(event.payload().testField());
  }
}
