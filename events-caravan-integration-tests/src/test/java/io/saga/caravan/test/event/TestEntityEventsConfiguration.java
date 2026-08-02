package io.saga.caravan.test.event;

import io.saga.caravan.event.EntityEventsRegistration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class TestEntityEventsConfiguration {

  public static final String TEST_ENTITY = "test-entity";

  public static final String TEST_EVENT = "test-event";
  public static final String ANOTHER_TEST_EVENT = "another-test-event";

  @Bean
  public EntityEventsRegistration testEntityEventsRegistration() {
    return new EntityEventsRegistration(
        TEST_ENTITY,
        Map.of(
            TEST_EVENT, TestEventPayload.class,
            ANOTHER_TEST_EVENT, TestEventPayload.class));
  }
}
