package dev.baitursinov.caravan.test.event.sourcing.snapshotting;

import dev.baitursinov.caravan.event.EntityEventsRegistration;
import dev.baitursinov.caravan.test.event.sourcing.entity.calculator.NumberCarryingPayload;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class SnapshottingCalculatorEventsConfiguration {

  public static final String SNAPSHOTTING_CALCULATOR = "snapshotting-calculator";

  public static final String NUMBER_ADDED = "number-added";
  public static final String NUMBER_SUBTRACTED = "number-subtracted";

  @Bean
  public EntityEventsRegistration snapshottingCalculatorEventsRegistration() {
    return new EntityEventsRegistration(
        SNAPSHOTTING_CALCULATOR,
        Map.of(
            NUMBER_ADDED, NumberCarryingPayload.class,
            NUMBER_SUBTRACTED, NumberCarryingPayload.class));
  }
}
