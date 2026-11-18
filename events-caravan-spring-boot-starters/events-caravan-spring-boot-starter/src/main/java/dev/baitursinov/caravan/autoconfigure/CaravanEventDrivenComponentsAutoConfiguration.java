package dev.baitursinov.caravan.autoconfigure;

import dev.baitursinov.caravan.event.consumer.EventConsumer;
import dev.baitursinov.caravan.event.consumer.EventMessageConsumer;
import dev.baitursinov.caravan.event.consumer.handler.EventHandler;
import dev.baitursinov.caravan.event.consumer.handler.HandlerBasedEventConsumer;
import dev.baitursinov.caravan.event.serialization.EventDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = CaravanJacksonSerializationAutoConfiguration.class)
public class CaravanEventDrivenComponentsAutoConfiguration {

  @Bean
  @ConditionalOnBean(EventDeserializer.class)
  @ConditionalOnMissingBean
  public EventMessageConsumer eventMessageConsumer(EventDeserializer eventDeserializer,
                                                   EventConsumer eventConsumer) {
    return new EventMessageConsumer(eventDeserializer, eventConsumer);
  }

  @Bean
  @ConditionalOnMissingBean
  public EventConsumer eventConsumer(ObjectProvider<EventHandler<?>> eventHandlers) {
    return new HandlerBasedEventConsumer(eventHandlers.stream().toList());
  }
}
