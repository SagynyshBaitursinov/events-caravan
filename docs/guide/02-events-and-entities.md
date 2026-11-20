# Events and entities

- [Register your Events](#register-your-events)
- [Define an Event-sourced-entity](#define-an-event-sourced-entity)
- [Events without payload](#events-without-payload)
- [Produce events without an entity](#produce-events-without-an-entity)

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

## Events without payload

When the event name alone says everything that happened, register `NoPayload` as its payload class:

```java
new EntityEventsRegistration(
    "calculator",
    Map.of(
        "number-added", NumberCarryingPayload.class,
        "reset", NoPayload.class));
```

Record it with `recordEvent(eventName)` and apply it with an `Event<NoPayload>` parameter:

```java
public void reset() {
  recordEvent("reset");
}

@ApplyEvent("reset")
private void applyReset(Event<NoPayload> event) {
  this.currentNumber = 0;
}
```

> [!NOTE]
>- `payload()` of such an event is `NoPayload.INSTANCE`, never `null`.
>- Nothing is stored or sent for it: the `payload` attribute and JSON field are omitted, and `NoPayload.INSTANCE` is restored when the event is read back.
>- Such events are dispatched to `EventHandler<NoPayload>` handlers.

## Produce events without an entity

Some events are plain facts with no state to rebuild later, such as a received payment notification or a
user's login. You can produce them without an `EventSourcedEntity` or repository, in one of two ways:

|                          | Sequenced: `SequencedEventProducer`               | Unsequenced: `UnsequencedEventProducer`                                        |
|--------------------------|---------------------------------------------------|--------------------------------------------------------------------------------|
| Identified by            | `entityReference` + `sequenceNumber`              | `entityReference` + `naturalKey`                                               |
| Written to               | `EventStore`                                      | message broker, nothing is stored                                              |
| Duplicates on production | rejected with `DuplicateEventProductionException` | delivered again, consumers could deduplicate using `naturalKey`                |
| Batch                    | atomic: all or none                               | atomicity may be guaranteed by implementation, but not by reference SNS module |

Either way, events still have to be [registered](#register-your-events): an event whose name is not registered, or
whose payload class differs from the registered one, fails with `EventProductionException` before anything is
written.

### Sequenced events

Choose them when the fact has to be kept, or when producing it must be idempotent at the source.

```java

@Service
@RequiredArgsConstructor
public class PaymentNotifications {

  private final SequencedEventProducer sequencedEventProducer;

  public void paymentReceived(String paymentId, long amount) {
    sequencedEventProducer.produce(
        Event.sequenced()
            .entityReference(new EntityReference("payment", paymentId))
            .eventName("received")
            .sequenceNumber(1)
            .timestamp(ZonedDateTime.now())
            .payload(new PaymentReceivedPayload(amount))
            .build());
  }
}
```

> [!NOTE]
>- You choose the `sequenceNumber`. It starts at 1 and must increase by one for each later event of the same entity.
   > Giving each fact its own entity id with sequence number 1 makes production idempotent: producing the same fact
   > again fails with `DuplicateEventProductionException` instead of storing a duplicate.
>
>- `DuplicateEventProductionException` means an event with the same entity and sequence number already exists:
   > either a retry of an already produced event, or a concurrent write to the same entity. Do not blindly retry
   > with the next sequence number; re-read the entity's events from `EventStore` and decide.
>
>- `produce(List)` writes all events atomically: all or none. The DynamoDB implementation allows at most 100 events
   > per call.
>
>- If you need to rebuild state from an entity's events, or to enforce rules across them, define an
   > [Event-sourced-entity](#define-an-event-sourced-entity) and save it through a repository instead.

### Unsequenced events

Choose them for fire-and-forget facts and commands to other services, where nobody relies on an event being the N-th
thing that happened to its entity and nobody needs to load it back later to derive entity's state.

```java

@Service
@RequiredArgsConstructor
public class LoginTracking {

  private final UnsequencedEventProducer unsequencedEventProducer;

  public void userLoggedIn(String userId, String sessionId) {
    unsequencedEventProducer.produce(
        Event.unsequenced()
            .entityReference(new EntityReference("user", userId))
            .eventName("user-logged-in")
            .naturalKey(sessionId)
            .timestamp(ZonedDateTime.now())
            .payload(new UserLoggedInPayload(sessionId))
            .build());
  }
}
```

> [!IMPORTANT]
>- Derive the `naturalKey` from the fact itself (a session id, an incoming request or message id), never generate
   > a random one per call: a retry must carry the same `naturalKey`, so that consumers can recognize the duplicate. It
   > has to be unique among events of the same name and entity.
>
>- The framework's core does not deduplicate by `naturalKey`. Every production of an event is delivered, and consumers that
   > must not process it twice have to check the `naturalKey` themselves.
>
>- In reference implementation unsequenced events are published synchronously; once `produce` returns, the broker has accepted it. On
   > `EventProductionException` events may or may not have been published, so retry them with the same `naturalKey`s.
>
>- Unsequenced events cannot be applied on an `EventSourcedEntity` via `@ApplyEvent`, and do not put entities into the
   > [Entity-stream](05-entity-stream.md).

---
Previous: [Modules and dependencies](01-modules-and-dependencies.md) · Next: [Repositories and snapshots](03-repositories-and-snapshots.md)
