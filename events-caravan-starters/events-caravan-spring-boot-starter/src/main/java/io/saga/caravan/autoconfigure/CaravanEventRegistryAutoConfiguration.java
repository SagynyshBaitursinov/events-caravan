package io.saga.caravan.autoconfigure;

import io.saga.caravan.event.EntityEventsRegistration;
import io.saga.caravan.event.EntityEventsRegistrationValidator;
import io.saga.caravan.event.EventPayloadClassMappingKeeper;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
public class CaravanEventRegistryAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public EventPayloadClassMappingKeeper eventPayloadClassMap(
      ObjectProvider<EntityEventsRegistration> entityEventsRegistrations) {

    return EventPayloadClassMappingKeeper.create(entityEventsRegistrations.stream().toList());
  }

  @Bean
  @ConditionalOnMissingBean
  public EntityEventsRegistrationValidator entityNamesValidator(
      ObjectProvider<EntityEventsRegistration> entityEventsRegistrations) {

    return new EntityEventsRegistrationValidator(entityEventsRegistrations.stream().toList());
  }

  @Bean
  public SmartInitializingSingleton entityEventsRegistrationValidationTrigger(
      EntityEventsRegistrationValidator entityEventsRegistrationValidator) {

    return entityEventsRegistrationValidator::validateAll;
  }
}
