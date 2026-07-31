package io.saga.caravan.autoconfigure;

import io.saga.caravan.event.EventPayloadClassMappingKeeper;
import io.saga.caravan.event.consumer.EventConsumer;
import io.saga.caravan.event.consumer.EventMessageConsumer;
import io.saga.caravan.event.consumer.handler.EventHandler;
import io.saga.caravan.event.consumer.handler.HandlerBasedEventConsumer;
import io.saga.caravan.event.producer.EventProducer;
import io.saga.caravan.event.producer.ValidatingEventProducer;
import io.saga.caravan.event.serialization.EventDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@AutoConfiguration(after = {
    CaravanEventRegistryAutoConfiguration.class,
    CaravanJacksonSerializationAutoConfiguration.class
})
public class CaravanEventDrivenComponentsAutoConfiguration {

  @Bean
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

  @Bean
  @Primary
  public EventProducer validatingEventProducer(EventProducer eventProducer,
                                               EventPayloadClassMappingKeeper eventPayloadClassMappingKeeper) {
    return new ValidatingEventProducer(eventProducer, eventPayloadClassMappingKeeper);
  }
}
