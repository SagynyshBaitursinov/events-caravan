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
    populateSnapshotTakerMap(snapshotTakers);
  }

  private void populateSnapshotTakerMap(List<SnapshotTaker<? extends EventSourcedEntity, ?>> snapshotTakers) {
    snapshotTakers.forEach(snapshotTaker -> {
      if (snapshotTaker.frequencyOfSnapshots() <= 0) {
        throw new EventSourcedEntitySetupException(
            "frequencyOfSnapshots is not positive in snapshotTaker %s"
                .formatted(snapshotTaker.getClass()));
      }

      if (snapshotTakerMap.containsKey(snapshotTaker.entityClass())) {
        throw new EventSourcedEntitySetupException(
            "Duplicate snapshot taker found for entity %s"
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

    validateDuplication(entityName);

    applyEventMethodPayloadsValidator.validate(entityName, entityClass);

    entityNames.add(entityName);
  }

  private void validateDuplication(String entityName) {
    if (entityNames.contains(entityName)) {
      throw new EventSourcedEntitySetupException(
          "Repository for entityName=%s already exists".formatted(entityName));
    }
  }
}
