package dev.baitursinov.caravan.autoconfigure;

import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.producer.EventProducer;
import dev.baitursinov.caravan.event.sourcing.EventSourcedEntity;
import dev.baitursinov.caravan.event.sourcing.EventSourcingRepositoryContext;
import dev.baitursinov.caravan.event.sourcing.EventStore;
import dev.baitursinov.caravan.event.sourcing.applying.ApplyEventMethodPayloadsValidator;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotStore;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotTaker;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = CaravanEventDrivenComponentsAutoConfiguration.class)
public class CaravanEventSourcingAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public ApplyEventMethodPayloadsValidator applyEventMethodPayloadsValidator(
      EntityEventsRegistry entityEventsRegistry) {

    return new ApplyEventMethodPayloadsValidator(entityEventsRegistry);
  }

  @Bean
  @ConditionalOnMissingBean
  public EventSourcingRepositoryContext eventSourcingRepositoryContext(
      EventStore eventStore,
      EventProducer eventProducer,
      SnapshotStore snapshotStore,
      ApplyEventMethodPayloadsValidator applyEventMethodPayloadsValidator,
      ObjectProvider<SnapshotTaker<? extends EventSourcedEntity, ?>> snapshotTakers) {

    return new EventSourcingRepositoryContext(
        eventStore,
        eventProducer,
        snapshotStore,
        applyEventMethodPayloadsValidator,
        snapshotTakers.stream().toList());
  }
}
