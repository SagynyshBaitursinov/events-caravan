package dev.baitursinov.caravan.test.event.handler;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.consumer.handler.EventHandler;
import dev.baitursinov.caravan.test.event.driven.TestFieldContainingEventPayload;
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
