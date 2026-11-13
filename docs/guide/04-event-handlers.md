# Event handlers

Handlers are matched by payload type, then filtered by `isOfInterest` (typically on the `EventType`):

```java

@Component
public class NumberAddedHandler implements EventHandler<NumberCarryingPayload> {

  private static final EventType INTERESTED_EVENT_TYPE = new EventType("calculator", "number-added");

  @Override
  public boolean isOfInterest(Event<NumberCarryingPayload> event) {
    return INTERESTED_EVENT_TYPE.equals(event.eventType());
  }

  @Override
  public void handle(Event<NumberCarryingPayload> event) {
    // update a query-model, execute a command, ...
  }
}
```

> [!NOTE]
>- Handlers are registered automatically by Spring Boot starters.
>- Handlers must be idempotent and tolerant of out-of-order arrival, see
   > [Event consumption](../architecture.md#event-consumption) and
   > [Compromise #2](../compromises.md#2-at-least-once-unordered-delivery).

---
Previous: [Repositories and snapshots](03-repositories-and-snapshots.md) · Next: [Entity-stream](05-entity-stream.md)
