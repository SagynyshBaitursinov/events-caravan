package io.saga.caravan.config;

import io.saga.caravan.messaging.MessagingProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SqsPollingConfiguration {

  @Bean
  public MessagingProperties sqsPollingProperties() {
    return MessagingProperties.builder()
        .concurrency(20)
        .maxPollSize(10)
        .minPollSize(3)
        .pollWaitSeconds(5)
        .gracefulShutdownSeconds(0)
        .build();
  }
}
