package io.saga.caravan.test.event.sourcing.entity.account;

import io.saga.caravan.event.EntityEventsRegistration;
import io.saga.caravan.test.event.sourcing.entity.calculator.NumberCarryingPayload;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class AccountEventsConfiguration {

  public static final String ACCOUNT = "account";
  public static final String RECEIVED_MONEY = "received-money";

  @Bean
  public EntityEventsRegistration entityEventsRegistration() {
    return new EntityEventsRegistration(
        ACCOUNT,
        Map.of(RECEIVED_MONEY, NumberCarryingPayload.class));
  }
}
