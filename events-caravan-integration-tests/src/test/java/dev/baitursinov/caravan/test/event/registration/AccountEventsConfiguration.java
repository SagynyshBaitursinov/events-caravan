package dev.baitursinov.caravan.test.event.registration;

import dev.baitursinov.caravan.event.EntityEventsRegistration;
import dev.baitursinov.caravan.test.value.NumberCarryingPayload;
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
