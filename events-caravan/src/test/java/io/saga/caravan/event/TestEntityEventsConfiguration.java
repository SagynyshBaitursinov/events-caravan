package io.saga.caravan.event;

import io.saga.caravan.event.payload.EventPayloadRegistration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class TestEntityEventsConfiguration {

  public static final String TEST_ENTITY = "test-entity";

  public static final String TEST_EVENT = "test-event";
  public static final String ANOTHER_TEST_EVENT = "another-test-event";

  @Bean
  public EventPayloadRegistration testEntityEventsRegistration() {
    return new EventPayloadRegistration() {

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
      public boolean isIncomingSubscriptionActive() {
        return true;
      }
    };
  }
}
