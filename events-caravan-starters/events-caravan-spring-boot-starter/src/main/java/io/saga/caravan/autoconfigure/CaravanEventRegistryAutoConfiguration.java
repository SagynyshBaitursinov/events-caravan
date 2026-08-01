package io.saga.caravan.autoconfigure;

import io.saga.caravan.event.EntityEventsRegistration;
import io.saga.caravan.event.EntityEventsRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
public class CaravanEventRegistryAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public EntityEventsRegistry entityEventsRegistry(
      ObjectProvider<EntityEventsRegistration> entityEventsRegistrations) {

    return EntityEventsRegistry.createFor(entityEventsRegistrations.stream().toList());
  }
}
