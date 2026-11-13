# Compromises

> [!NOTE]
> Per [Principle #5](philosophy.md), every guarantee traded away for scalability is listed here
> explicitly, together with how to compensate for it.

## 1. No global sequence of events

Compared to another popular event-sourcing framework, [Axon](comparison-axon.md), 
Events-caravan does not maintain a global sequence of events across all entities. Therefore, there's no global iterable 
stream of events. Such a compromise is taken because otherwise a global ordering mechanism and single writer node would be necessary; 
these would potentially become a bottleneck of the system on write path. Events-caravan avoids this by partitioning events at the entity level.

This approach has the following drawbacks:
- CQRS query-model reflections cannot be re-built by retriggering `EventHandlers` for all historic events.
- There's no built-in event-store in case if events across many entities need to be republished.

Events-caravan ships a global stream of entity references to compensate;
See [Set up an optional Entity-stream](guide/05-entity-stream.md).

## 2. At-least-once, unordered delivery

Events delivery to consumers is at-least _(not exactly)_ once and unordered. Consumers own idempotency and, if
needed, ordering. Every event message carries a gapless `sequenceNumber` starting at 1 per `entityReference`, which
might be helpful for deduplication and reordering by an application. Ultimate deduplication and reordering is
recommended to be taken care of by an underlying domain layer. e.g. Do not brew _coffee#7_, if the _coffee#7_ is
already in status _delivered_.

## 3. No cross-entity transactions are required from adapters

As per [Principle #2](philosophy.md) Events-caravan core is satisfied with eventual consistency. If underlying database
does not provide transaction coordination, but there's a necessity to maintain consistency across multiple Entities,
applications must respond to this. The answer to this challenge is [Saga design pattern](https://microservices.io/patterns/data/saga.html).
Each saga step is an `EventHandler` that records the next event, and a process needing central state can be modeled as an `EventSourcedEntity`.

## 4. Loaded state may be stale

Application code must be prepared for the loaded Entity state being stale due to eventual consistency across database replicas.
If new events are attempted to be produced on top of stale state, underlying database will detect the issue and throw `DuplicateEventProductionException`.
In such case, or if stale state is detected by application code earlier, process should fail and be retried.
Adapters may offer strongly consistent reads to reduce this, e.g. the DynamoDB adapter's
[`consistent-read`](aws-reference-implementation.md#consistent-reads).
