package dev.baitursinov.caravan.test.event.sourcing.entity.calculator;

import dev.baitursinov.caravan.event.EntityEventsRegistration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class CalculatorEventsConfiguration {

  public static final String CALCULATOR = "calculator";

  public static final String NUMBER_ADDED = "number-added";
  public static final String NUMBER_SUBTRACTED = "number-subtracted";

  @Bean
  public EntityEventsRegistration calculatorEventsRegistration() {
    return new EntityEventsRegistration(
        CALCULATOR,
        Map.of(
            NUMBER_ADDED, NumberCarryingPayload.class,
            NUMBER_SUBTRACTED, NumberCarryingPayload.class));
  }
}
