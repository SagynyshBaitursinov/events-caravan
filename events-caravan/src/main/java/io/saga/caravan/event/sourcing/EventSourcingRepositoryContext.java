package io.saga.caravan.event.sourcing;

import io.saga.caravan.event.producer.EventProducer;
import io.saga.caravan.event.sourcing.applying.ApplyEventMethodPayloadsValidator;
import io.saga.caravan.event.sourcing.snapshot.SnapshotStore;
import io.saga.caravan.event.sourcing.snapshot.SnapshotTaker;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EventSourcingRepositoryContext {

  private final Set<String> entityNames = new HashSet<>();
  private final Set<Class<? extends EventSourcedEntity>> entityClasses = new HashSet<>();
  private final EventStore eventStore;
  private final EventProducer eventProducer;
  private final SnapshotStore snapshotStore;
  private final ApplyEventMethodPayloadsValidator applyEventMethodPayloadsValidator;
  private final Map<Class<? extends EventSourcedEntity>, SnapshotTaker<? extends EventSourcedEntity, ?>> snapshotTakerMap;

  public EventSourcingRepositoryContext(EventStore eventStore,
                                        EventProducer eventProducer,
                                        SnapshotStore snapshotStore,
                                        ApplyEventMethodPayloadsValidator applyEventMethodPayloadsValidator,
                                        List<SnapshotTaker<? extends EventSourcedEntity, ?>> snapshotTakers) {
    this.eventStore = eventStore;
    this.eventProducer = eventProducer;
    this.snapshotStore = snapshotStore;
    this.applyEventMethodPayloadsValidator = applyEventMethodPayloadsValidator;

    this.snapshotTakerMap = new HashMap<>(snapshotTakers.size());
    snapshotTakers.forEach(snapshotTaker -> {
      if (snapshotTakerMap.containsKey(snapshotTaker.entityClass())) {
        throw new EventSourcedEntitySetupException(
            "Duplicate snapshot taker found for class %s"
                .formatted(snapshotTaker.entityClass()));
      }
      snapshotTakerMap.put(snapshotTaker.entityClass(), snapshotTaker);
    });
  }

  EventStore eventStore() {
    return eventStore;
  }

  EventProducer eventProducer() {
    return eventProducer;
  }

  SnapshotStore snapshotStore() {
    return snapshotStore;
  }

  @SuppressWarnings("unchecked")
  @Nullable
  <T extends EventSourcedEntity> SnapshotTaker<T, ?> snapshotTakerFor(Class<T> entityClass) {
    return (SnapshotTaker<T, ?>) snapshotTakerMap.get(entityClass);
  }

  void register(EventSourcedRepository<? extends EventSourcedEntity> eventSourcedRepository) {
    String entityName = eventSourcedRepository.entityName();
    Class<? extends EventSourcedEntity> entityClass = eventSourcedRepository.entityClass();

    validateDuplication(entityName, entityClass);

    applyEventMethodPayloadsValidator.validate(entityName, entityClass);

    entityNames.add(entityName);
    entityClasses.add(entityClass);
  }

  private void validateDuplication(String entityName, Class<? extends EventSourcedEntity> entityClass) {
    if (entityNames.contains(entityName)
        || entityClasses.contains(entityClass)) {
      throw new EventSourcedEntitySetupException(
          "entityName=%s or entityClass=%s are duplicated"
              .formatted(entityName, entityClass));
    }
  }
}
