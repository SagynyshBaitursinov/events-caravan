package dev.baitursinov.caravan.test.event.registration;

import dev.baitursinov.caravan.event.EntityEventsRegistration;
import dev.baitursinov.caravan.test.value.NumberAdditionRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class NumberAdditionEventConfiguration {

  public static final String NUMBER_ADDITION_REQUEST = "number-addition-request";
  public static final String RECEIVED = "received";

  @Bean
  public EntityEventsRegistration numberAdditionRequestEventRegistration() {
    return new EntityEventsRegistration(
        NUMBER_ADDITION_REQUEST,
        Map.of(RECEIVED, NumberAdditionRequest.class));
  }
}
