package io.saga.caravan.test.event.handler;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.consumer.handler.EventHandler;
import io.saga.caravan.test.event.driven.TestFieldContainingEventPayload;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

@Component
@Getter
public class InterestingTestFieldSavingEventHandler implements EventHandler<TestFieldContainingEventPayload> {

  public static final String INTERESTING = "interesting";

  private final Set<String> savedFields = new HashSet<>();

  @Override
  public boolean isOfInterest(Event<TestFieldContainingEventPayload> event) {
    return event.payload().testField().startsWith(INTERESTING);
  }

  @Override
  public void handle(Event<TestFieldContainingEventPayload> event) {
    savedFields.add(event.payload().testField());
  }
}
