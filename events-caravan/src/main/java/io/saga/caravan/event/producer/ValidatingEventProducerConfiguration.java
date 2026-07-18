package io.saga.caravan.event.producer;

import io.saga.caravan.event.EventType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.Map;

@Configuration
public class ValidatingEventProducerConfiguration {

  @Bean
  @Primary
  public EventProducer validatingEventProducer(
      EventProducer eventProducer,
      Map<EventType, Class<?>> eventPayloadClassMap) {

    return new ValidatingEventProducer(eventProducer, eventPayloadClassMap);
  }
}
