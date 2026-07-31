package io.saga.caravan.autoconfigure;

import io.saga.caravan.event.EventPayloadClassMappingKeeper;
import io.saga.caravan.event.producer.EventProducer;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import io.saga.caravan.event.sourcing.EventSourcingRepositoryContext;
import io.saga.caravan.event.sourcing.EventStore;
import io.saga.caravan.event.sourcing.applying.ApplyEventMethodPayloadsValidator;
import io.saga.caravan.event.sourcing.snapshot.SnapshotStore;
import io.saga.caravan.event.sourcing.snapshot.SnapshotTaker;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = CaravanEventDrivenComponentsAutoConfiguration.class)
public class CaravanEventSourcingAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public ApplyEventMethodPayloadsValidator applyEventMethodPayloadsValidator(
      EventPayloadClassMappingKeeper eventPayloadClassMappingKeeper) {

    return new ApplyEventMethodPayloadsValidator(eventPayloadClassMappingKeeper);
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
