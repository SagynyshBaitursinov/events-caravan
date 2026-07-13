package io.saga.caravan.event.sourcing.entity.calculator;

import io.saga.caravan.event.payload.EventPayloadRegistration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class CalculatorEventsConfiguration {

  public static final String CALCULATOR = "calculator";

  public static final String NUMBER_ADDED = "number-added";
  public static final String NUMBER_SUBTRACTED = "number-subtracted";

  @Bean
  public EventPayloadRegistration calculatorEventsRegistration() {
    return new EventPayloadRegistration() {

      @Override
      public String entityName() {
        return CALCULATOR;
      }

      @Override
      public Map<String, Class<?>> eventToPayloadClass() {
        return Map.of(
            NUMBER_ADDED, NumberCarryingPayload.class,
            NUMBER_SUBTRACTED, NumberCarryingPayload.class);
      }

      @Override
      public boolean isIncomingSubscriptionActive() {
        return true;
      }
    };
  }
}
