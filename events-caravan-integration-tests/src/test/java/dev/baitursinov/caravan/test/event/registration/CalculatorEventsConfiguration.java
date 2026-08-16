package dev.baitursinov.caravan.test.event.registration;

import dev.baitursinov.caravan.event.EntityEventsRegistration;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamRegistration;
import dev.baitursinov.caravan.event.sourcing.entity.stream.TimeBucket;
import dev.baitursinov.caravan.test.value.NumberCarryingPayload;
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

  @Bean
  public EntityStreamRegistration calculatorEntityStreamRegistration() {
    return new EntityStreamRegistration(CALCULATOR, TimeBucket.MONTHLY, 4);
  }
}
