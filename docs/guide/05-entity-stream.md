# Set up an optional Entity-stream

Per [Compromise #1](../compromises.md#1-no-global-sequence-of-events), Events-caravan keeps no global, cross-entity
stream of events. Events are sorted in scope of each entity only. So in case if all entities or events must be iterated
over (e.g. CQRS query-models need to be rebuilt), an optional functionality Entity-stream compensates for absence of
global sequence of events.

With the DynamoDB starter, setting `caravan.event.sourcing.entity-stream.dynamo-db.table-name` property autoconfigures a
`DynamoDbBasedEntityStreamWriter`, and its caller `EntityStreamWritingEventHandler`
(see [Configuration properties](../aws-reference-implementation.md#configuration-properties)).

Which entities are written into the stream, and at which granularity, is declared per `entityName` via an
`EntityStreamRegistration` bean:

```java

@Configuration
public class CalculatorEventsConfiguration {

  @Bean
  public EntityStreamRegistration calculatorEntityStreamRegistration() {
    return new EntityStreamRegistration("calculator", TimeBucket.MONTHLY, 4);
  }
}
```

`EntityStreamWritingEventHandler` partitions entity references by their:

- `entityName`
- first event timestamp's `timeBucket` granularity
- N slices (`sliceCount`) by a hash _(FNV-1a 64-bit)_ of `entityId` within each bucket

then passes the derived time bucket and slice locations along with the entity reference to the configured
`EntityStreamWriter`. This way the Entity-stream's storage is enabled to scale horizontally without hot partitions.

> [!IMPORTANT]
>- `EntityStreamRegistration`'s `timeBucket` and `sliceCount` must stay fixed per `entityName` once the Entity-stream
   > table has entities of it in it, as they define how the stream is sliced. Re-creating the stream for already written
   > data changing their slicing parameters may be an expensive operation. Thus, it's important to set values to have
   > fine enough partitions in anticipation of the load.
>
>- This functionality is entirely optional; if `EntityStreamRegistration` is not set up, entities won't be populated 
   > for the `entityName`. Setting it up later will start populating only the new entities into the stream.

> [!NOTE]
>- Entities whose `entityName` has no `EntityStreamRegistration` are not written into the Entity-stream, so different
   > entities can be onboarded onto the Entity-stream independently, at whatever granularity suits their event
   > production frequency.
>
>- Setting the values too generously (small time-buckets and high slice count) will lead to more
   > partitions, which would be empty or underfilled, when Entity creation is not frequent enough. Consequently,
   > iterating over the Entity-stream will be more costly due to some DB queries will result in no or few entries.
>
>- Wiring of components is automatic once beans exist: `EntityStreamRegistration` beans are collected into an
   > `EntityStreamRegistry`, and `EntityStreamWritingEventHandler` is registered by the Spring boot starter and calls
   > it for every registered entity's first event (`sequenceNumber == 1`).

---
Previous: [Event handlers](04-event-handlers.md) · Next: [AWS reference implementation](../aws-reference-implementation.md)
