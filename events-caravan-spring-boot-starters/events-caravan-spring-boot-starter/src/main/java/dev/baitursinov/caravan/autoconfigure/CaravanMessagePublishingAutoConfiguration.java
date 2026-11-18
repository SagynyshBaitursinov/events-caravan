package dev.baitursinov.caravan.autoconfigure;

import dev.baitursinov.caravan.event.producer.MessagePublishingUnsequencedEventWriter;
import dev.baitursinov.caravan.event.producer.UnsequencedEventWriter;
import dev.baitursinov.caravan.event.publisher.EventMessagePublisher;
import dev.baitursinov.caravan.event.serialization.EventSerializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(before = CaravanEventProducersAutoConfiguration.class)
public class CaravanMessagePublishingAutoConfiguration {

  @Bean
  @ConditionalOnBean(EventMessagePublisher.class)
  @ConditionalOnMissingBean
  public UnsequencedEventWriter messagePublishingUnsequencedEventWriter(EventSerializer eventSerializer,
                                                                        EventMessagePublisher eventMessagePublisher) {
    return new MessagePublishingUnsequencedEventWriter(eventSerializer, eventMessagePublisher);
  }
}
