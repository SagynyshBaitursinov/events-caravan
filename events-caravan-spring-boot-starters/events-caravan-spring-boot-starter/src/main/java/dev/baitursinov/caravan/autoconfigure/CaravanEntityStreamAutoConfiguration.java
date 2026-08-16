package dev.baitursinov.caravan.autoconfigure;

import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamRegistration;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamRegistry;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamWriter;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamWritingEventHandler;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Wires the entity stream into event consumption: when an application configures an
 * {@link EntityStreamWriter}, every entity's first event gets written in stream automatically,
 * provided its entityName has an {@link EntityStreamRegistration}.
 */
@AutoConfiguration(
    afterName = "dev.baitursinov.caravan.autoconfigure.dynamodb.CaravanDynamoDbAutoConfiguration",
    before = CaravanEventDrivenComponentsAutoConfiguration.class)
public class CaravanEntityStreamAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public EntityStreamRegistry entityStreamRegistry(
      ObjectProvider<EntityStreamRegistration> entityStreamRegistrations) {

    return EntityStreamRegistry.createFor(entityStreamRegistrations.stream().toList());
  }

  @Bean
  @ConditionalOnBean(EntityStreamWriter.class)
  @ConditionalOnMissingBean
  public EntityStreamWritingEventHandler entityStreamWritingEventHandler(
      EntityStreamWriter entityStreamWriter, EntityStreamRegistry entityStreamRegistry) {

    return new EntityStreamWritingEventHandler(entityStreamWriter, entityStreamRegistry);
  }
}
