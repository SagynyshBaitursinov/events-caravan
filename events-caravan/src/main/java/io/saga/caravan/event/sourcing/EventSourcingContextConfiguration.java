package io.saga.caravan.event.sourcing;

import io.saga.caravan.event.EventType;
import io.saga.caravan.event.producer.EventProducer;
import io.saga.caravan.event.sourcing.applying.ApplyEventMethodPayloadsValidator;
import io.saga.caravan.event.sourcing.snapshot.SnapshotStore;
import io.saga.caravan.event.sourcing.snapshot.SnapshotTaker;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

@Configuration
public class EventSourcingContextConfiguration {

  @Bean
  public EventSourcingRepositoryContext eventSourcedRepositoriesContext(
      EventStore eventStore,
      EventProducer eventProducer,
      SnapshotStore snapshotStore,
      List<SnapshotTaker<? extends EventSourcedEntity, ?>> snapshotTakers,
      ApplyEventMethodPayloadsValidator applyEventMethodPayloadsValidator) {

    return new EventSourcingRepositoryContext(
        eventStore,
        eventProducer,
        snapshotStore,
        applyEventMethodPayloadsValidator,
        snapshotTakers);
  }

  @Bean
  public ApplyEventMethodPayloadsValidator applyEventMethodsValidator(
      Map<EventType, Class<?>> eventPayloadClassMap) {

    return new ApplyEventMethodPayloadsValidator(eventPayloadClassMap);
  }
}
