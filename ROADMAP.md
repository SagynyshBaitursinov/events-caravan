# Roadmap

1. Introduce a mechanism of iterating over [Entity-Stream](docs/guide/05-entity-stream.md) and re-building CQRS
   query-models.
2. Introduce events flow traceability, metrics.
3. Introduce Event versioning and upcasting mechanism.
4. Develop adapters for PostgreSQL/ScyllaDB + Debezium + Kafka protocol.
5. Introduce Multi region scalability.
6. Provide possibility of taking snapshots async from thread of `EventSourcedRepository.save(...)`.
7. Provide optional capability not to send Event payload into message broker, but only reference to be used for fetching
   the event details from the event-store.
8. Support to have `@ApplyEvent` parameter as unwrapped payload (without `Event<T>`).
9. Add possibility to have `Void` payload of events and no Event parameter to `@ApplyEvent`.
10. Introduce grouping of EventHandlers per queue groups.
11. Loading events directly via a provided API could be interesting for apps.
12. Consider possibility of adding Enums as Event names.
