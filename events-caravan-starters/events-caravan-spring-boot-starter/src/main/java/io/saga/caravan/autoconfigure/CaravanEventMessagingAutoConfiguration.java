package io.saga.caravan.autoconfigure;

import io.saga.caravan.event.EntityEventsRegistration;
import io.saga.caravan.event.consumer.queue.SubscribedEntityQueueNamesKeeper;
import io.saga.caravan.messaging.MessagingProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = CaravanEventDrivenComponentsAutoConfiguration.class)
@EnableConfigurationProperties(CaravanMessagingConfigurationProperties.class)
public class CaravanEventMessagingAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public MessagingProperties eventQueueingProperties(CaravanMessagingConfigurationProperties properties) {
    return properties.toMessagingProperties();
  }

  @Bean
  @ConditionalOnMissingBean
  public SubscribedEntityQueueNamesKeeper subscribedEntityQueueNamesKeeper(
      ObjectProvider<EntityEventsRegistration> entityEventsRegistrations,
      CaravanMessagingConfigurationProperties properties) {

    return SubscribedEntityQueueNamesKeeper.create(
        entityEventsRegistrations.stream().toList(),
        properties.queueNamePrefix());
  }
}
