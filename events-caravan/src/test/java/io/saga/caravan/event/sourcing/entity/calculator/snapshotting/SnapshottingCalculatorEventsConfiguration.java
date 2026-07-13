package io.saga.caravan.event.sourcing.entity.calculator.snapshotting;

import io.saga.caravan.event.payload.EventPayloadRegistration;
import io.saga.caravan.event.sourcing.entity.calculator.NumberCarryingPayload;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class SnapshottingCalculatorEventsConfiguration {

  public static final String SNAPSHOTTING_CALCULATOR = "snapshotting-calculator";

  public static final String NUMBER_ADDED = "number-added";
  public static final String NUMBER_SUBTRACTED = "number-subtracted";

  @Bean
  public EventPayloadRegistration snapshottingCalculatorEventsRegistration() {
    return new EventPayloadRegistration() {

      @Override
      public String entityName() {
        return SNAPSHOTTING_CALCULATOR;
      }

      @Override
      public Map<String, Class<?>> eventToPayloadClass() {
        return Map.of(
            NUMBER_ADDED, NumberCarryingPayload.class,
            NUMBER_SUBTRACTED, NumberCarryingPayload.class);
      }

      @Override
      public boolean isIncomingSubscriptionActive() {
        return false;
      }
    };
  }
}
