package io.saga.caravan.event;

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
    return new EntityEventsRegistration() {

      @Override
      public String entityName() {
        return TEST_ENTITY;
      }

      @Override
      public Map<String, Class<?>> eventToPayloadClass() {
        return Map.of(
            TEST_EVENT, TestEventPayload.class,
            ANOTHER_TEST_EVENT, TestEventPayload.class);
      }

      @Override
      public boolean isSubscriptionActive() {
        return true;
      }
    };
  }
}
