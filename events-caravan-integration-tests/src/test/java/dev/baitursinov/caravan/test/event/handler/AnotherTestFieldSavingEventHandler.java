package dev.baitursinov.caravan.test.event.handler;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.consumer.handler.EventHandler;
import dev.baitursinov.caravan.test.value.TestEventPayload;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Getter
public class AnotherTestFieldSavingEventHandler implements EventHandler<TestEventPayload> {

  private final Set<String> savedFields = ConcurrentHashMap.newKeySet();

  @Override
  public boolean isOfInterest(Event<TestEventPayload> event) {
    return true;
  }

  @Override
  public void handle(Event<TestEventPayload> event) {
    savedFields.add(event.payload().anotherTestField());
  }
}
