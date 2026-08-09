package dev.baitursinov.caravan.test.event.handler;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.consumer.handler.EventHandler;
import dev.baitursinov.caravan.test.value.TestEventPayload;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

@Component
@Getter
public class AnotherTestFieldSavingEventHandler implements EventHandler<TestEventPayload> {

  private final Set<String> savedFields = new HashSet<>();

  @Override
  public boolean isOfInterest(Event<TestEventPayload> event) {
    return true;
  }

  @Override
  public void handle(Event<TestEventPayload> event) {
    savedFields.add(event.payload().anotherTestField());
  }
}
