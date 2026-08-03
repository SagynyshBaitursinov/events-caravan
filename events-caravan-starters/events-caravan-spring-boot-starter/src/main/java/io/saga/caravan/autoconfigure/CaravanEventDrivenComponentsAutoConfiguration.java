package io.saga.caravan.autoconfigure;

import io.saga.caravan.event.EntityEventsRegistry;
import io.saga.caravan.event.consumer.EventConsumer;
import io.saga.caravan.event.consumer.EventMessageConsumer;
import io.saga.caravan.event.consumer.handler.EventHandler;
import io.saga.caravan.event.consumer.handler.HandlerBasedEventConsumer;
import io.saga.caravan.event.producer.EventProducer;
import io.saga.caravan.event.producer.ValidatingEventProducer;
import io.saga.caravan.event.serialization.EventDeserializer;
import org.aopalliance.intercept.MethodInterceptor;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import java.lang.reflect.InvocationTargetException;

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
  static BeanPostProcessor eventProducerValidatingBeanPostProcessor(
      ObjectProvider<EntityEventsRegistry> entityEventsRegistry) {

    return new BeanPostProcessor() {

      @Override
      public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (!(bean instanceof EventProducer eventProducer) || bean instanceof ValidatingEventProducer) {
          return bean;
        }

        var validatingEventProducer =
            new ValidatingEventProducer(eventProducer, entityEventsRegistry.getObject());
        var proxyFactory = new ProxyFactory(bean);
        proxyFactory.addAdvice((MethodInterceptor) invocation -> {
          if (invocation.getMethod().getDeclaringClass() != EventProducer.class) {
            return invocation.proceed();
          }
          try {
            return invocation.getMethod().invoke(validatingEventProducer, invocation.getArguments());
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });

        return proxyFactory.getProxy();
      }
    };
  }
}
