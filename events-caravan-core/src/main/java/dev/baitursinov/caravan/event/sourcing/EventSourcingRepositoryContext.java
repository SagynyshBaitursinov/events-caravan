package dev.baitursinov.caravan.event.sourcing;

import dev.baitursinov.caravan.event.producer.EventProducer;
import dev.baitursinov.caravan.event.sourcing.applying.ApplyEventMethodPayloadsValidator;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotStore;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotTaker;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The shared dependencies of every {@link EventSourcedRepository} in an application: the
 * {@link EventStore}, {@link EventProducer} and {@link SnapshotStore} to use, plus the
 * {@link SnapshotTaker}s available for specific entity types. Applications construct one
 * instance and pass it to each of their {@link EventSourcedRepository} subclasses' constructors;
 * it also enforces that at most one repository is registered per entity name.
 */
public class EventSourcingRepositoryContext {

  private final Set<String> entityNames = new HashSet<>();
  private final EventStore eventStore;
  private final EventProducer eventProducer;
  private final SnapshotStore snapshotStore;
  private final ApplyEventMethodPayloadsValidator applyEventMethodPayloadsValidator;
  private final Map<Class<? extends EventSourcedEntity>, SnapshotTaker<? extends EventSourcedEntity, ?>> snapshotTakerMap;

  /**
   * @throws EventSourcedEntitySetupException if two snapshot takers are given for the same
   *                                          entity class, or one declares a non-positive
   *                                          snapshot frequency
   */
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
