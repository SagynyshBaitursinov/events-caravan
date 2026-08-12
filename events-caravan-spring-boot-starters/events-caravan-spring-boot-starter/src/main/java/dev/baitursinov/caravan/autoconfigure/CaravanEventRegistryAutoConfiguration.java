package dev.baitursinov.caravan.autoconfigure;

import dev.baitursinov.caravan.event.EntityEventsRegistration;
import dev.baitursinov.caravan.event.EntityEventsRegistry;
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
