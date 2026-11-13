# Repositories and snapshots

- [Set up an Event-sourced-repository](#set-up-an-event-sourced-repository)
- [Set up optional Snapshot-takers](#set-up-optional-snapshot-takers)

## Set up an Event-sourced-repository

```java

@Repository
public class CalculatorRepository extends EventSourcedRepository<Calculator> {

  public CalculatorRepository(EventSourcingRepositoryContext context) {
    super(Calculator.class, context);
  }

  @Override
  protected Calculator createWithBlankState(String entityId) {
    return new Calculator(entityId);
  }
}
```

Use it like any repository:

```java
public class CalculatorService {

  private final CalculatorRepository calculatorRepository;

  public CalculatorService(CalculatorRepository calculatorRepository) {
    this.calculatorRepository = calculatorRepository;
  }

  void addToMagicNumber() {
    var calculator = new Calculator("42");
    calculator.addNumber(10);
    calculatorRepository.save(calculator);

    Optional<Calculator> foundCalculator = calculatorRepository.findBy("42");
  }
}
```

> [!NOTE]
>- Entities in a blank state (no events recorded) cannot be saved or loaded.
>- Saving produces all events recorded since the entity was loaded.

## Set up optional Snapshot-takers

For entities with long histories, register a `SnapshotTaker` bean. An `EventSourcedRepository` then persists a snapshot
every N events defined by `frequencyOfSnapshots`. When the entity is loaded, the framework restores its state from the
latest snapshot plus later events instead of having to load and re-apply all events.

Snapshot writes are deliberately not atomic with event production to avoid the dual-write problem. A failed snapshot is
simply retaken at the next opportunity, and failure to take snapshot does not corrupt an entity's state.

```java

@Component
public class CalculatorSnapshotTaker extends SnapshotTaker<Calculator, CalculatorSnapshot> {

  public CalculatorSnapshotTaker() {
    super(Calculator.class, CalculatorSnapshot.class);
  }

  @Override
  public CalculatorSnapshot takeSnapshot(Calculator entity) {
    return new CalculatorSnapshot(entity.currentNumber());
  }

  @Override
  public Calculator recreateFromSnapshot(EntityReference entityReference,
                                         CalculatorSnapshot snapshotPayload) {
    var result = new Calculator(entityReference.entityId());
    result.currentNumber = snapshotPayload.currentNumber();
    return result;
  }

  @Override
  public int frequencyOfSnapshots() {
    return 50;
  }
}
```

> [!NOTE]
>- Spring Boot starter wires it into the `EventSourcingRepositoryContext` automatically.
>
>- SnapshotTaker in the provided example is located in the same package as Calculator entity and uses its
   > package-private fields, for the same reason as
   > [external `@EventApplier` classes](02-events-and-entities.md#keeping-apply-methods-outside-the-entity).

---
Previous: [Events and entities](02-events-and-entities.md) · Next: [Event handlers](04-event-handlers.md)
