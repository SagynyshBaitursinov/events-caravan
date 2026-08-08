package dev.baitursinov.caravan.test.event.handler;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.EventType;
import dev.baitursinov.caravan.event.consumer.handler.EventHandler;
import dev.baitursinov.caravan.test.event.driven.TestFieldContainingEventPayload;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

import static dev.baitursinov.caravan.test.event.driven.TestEntityEventsConfiguration.TEST_ENTITY;
import static dev.baitursinov.caravan.test.event.driven.TestEntityEventsConfiguration.TEST_EVENT;

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
