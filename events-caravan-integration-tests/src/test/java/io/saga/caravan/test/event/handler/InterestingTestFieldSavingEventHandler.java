package io.saga.caravan.test.event.handler;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.consumer.handler.EventHandler;
import io.saga.caravan.test.event.driven.TestFieldContainingEventPayload;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

import static io.saga.caravan.test.event.driven.TestEntityEventsConfiguration.TEST_ENTITY;
import static io.saga.caravan.test.event.driven.TestEntityEventsConfiguration.TEST_EVENT;

@Component
@Getter
public class InterestingTestFieldSavingEventHandler implements EventHandler<TestFieldContainingEventPayload> {

  public static final EventType INTERESTED_EVENT_TYPE = new EventType(TEST_ENTITY, TEST_EVENT);
  public static final String INTERESTING = "interesting";

  private final Set<String> savedFields = new HashSet<>();

  @Override
  public boolean isOfInterest(Event<TestFieldContainingEventPayload> event) {
    return event.eventType().equals(INTERESTED_EVENT_TYPE)
        && event.payload().testField().startsWith(INTERESTING);
  }

  @Override
  public void handle(Event<TestFieldContainingEventPayload> event) {
    savedFields.add(event.payload().testField());
  }
}
