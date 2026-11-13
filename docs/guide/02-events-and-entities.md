# Events and entities

- [Register your Events](#register-your-events)
- [Define an Event-sourced-entity](#define-an-event-sourced-entity)

## Register your Events

```java

@Configuration
public class CalculatorEventsConfiguration {

  @Bean
  public EntityEventsRegistration calculatorEventsRegistration() {
    return new EntityEventsRegistration(
        "calculator",
        Map.of(
            "number-added", NumberCarryingPayload.class,
            "number-subtracted", NumberCarryingPayload.class));
  }
}
```

> [!NOTE]
>- A registered entity does not have to be produced locally: you can register events produced by another application to
   > react to them in your application.
>
>- Entity and Event names are identified by explicit **Strings**, not Java class names, so payload classes can be
   > renamed and moved freely without breaking stored history.
>
>- The registry is built once at startup. Application validates every event produced or applied against the registry.
>
>- EntityEventsRegistration is picked up by Spring-boot automatically.

## Define an Event-sourced-entity

The matching `@ApplyEvent` method mutates in-memory state immediately when recording a new event and when the entity is
later loaded by replaying its historical events.

Entity state mutates further when new events are recorded by `EventSourcedEntity.recordEvent()`.

```java

@EntityName("calculator")
public class Calculator extends EventSourcedEntity {

  private final String id;

  long currentNumber = 0;

  public Calculator(String id) {
    this.id = id;
  }

  public long currentNumber() {
    return this.currentNumber;
  }

  public void addNumber(long number) {
    recordEvent("number-added", new NumberCarryingPayload(number));
  }

  @ApplyEvent("number-added")
  private void applyAddNumber(Event<NumberCarryingPayload> event) {
    this.currentNumber += event.payload().number();
  }

  @Override
  public String entityId() {
    return id;
  }
}
```

### Keeping apply methods outside the entity

> [!TIP]
>- Apply methods can also live outside the entity class (see `@ApplyEvent` and `@EventApplier`), keeping domain classes
   > free of state change mechanics.
>
>  It's recommended to place external @EventApplier classes in the same package as Entity in order to utilize
   > package-private fields/methods in order to mutate Entity's state while applying its events.
   > This helps Entity not to expose public getter/setter methods just for applying events without real domain behavior.

```java

@EntityName("calculator")
@EventApplier(CalculatorEventApplier.class)
public class Calculator extends EventSourcedEntity {

  private final String id;

  long currentNumber = 0; // package-private: mutated only by CalculatorEventApplier

  public Calculator(String id) {
    this.id = id;
  }

  public long currentNumber() {
    return this.currentNumber;
  }

  public void addNumber(long number) {
    recordEvent("number-added", new NumberCarryingPayload(number));
  }

  @Override
  public String entityId() {
    return id;
  }
}
```

```java
// same package as Calculator
final class CalculatorEventApplier {

  @ApplyEvent("number-added")
  static void applyAddNumber(Calculator calculator, Event<NumberCarryingPayload> event) {
    calculator.currentNumber += event.payload().number();
  }
}
```

> [!NOTE]
>- `@ApplyEvent` methods in an `@EventApplier` class must be `static` and take `(EntityClass entity, Event<PayloadClass> event)`
   > parameters. This is validated at startup.
>- `@EventApplier` accepts several classes, e.g. `@EventApplier({OrderPlacementApplier.class, OrderShippingApplier.class})`.
>- Each event name may have only one apply method across the entity class and all its `@EventApplier` classes.
   > Duplicates fail at startup.

---
Previous: [Modules and dependencies](01-modules-and-dependencies.md) · Next: [Repositories and snapshots](03-repositories-and-snapshots.md)
