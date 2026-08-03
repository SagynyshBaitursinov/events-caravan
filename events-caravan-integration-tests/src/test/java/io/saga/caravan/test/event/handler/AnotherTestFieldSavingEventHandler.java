package io.saga.caravan.test.event.handler;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.consumer.handler.EventHandler;
import io.saga.caravan.test.event.driven.TestEventPayload;
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
