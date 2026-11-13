# Philosophy in principles

1. **Enable horizontal scaling.** Anything that must be "the one" instance eventually becomes the ceiling, so the
   framework requires nothing global: no global events sequence, no locks, no requirement for a central process or transaction coordinator.
2. **Be satisfied with Eventual Consistency.** The framework does not require its database to provide atomic transaction coordination 
   beyond a single entity, or across database replicas.
   Every read and write is keyed by one entity, which enables data to be finely partitioned and sharded without constraints.
   See [Pat Helland's "Life beyond Distributed Transactions"](https://queue.acm.org/detail.cfm?id=3025012).
3. **Utilize simple and cheap technology features.** Prefer
   [smart endpoints and dumb pipes](https://martinfowler.com/articles/microservices.html#SmartEndpointsAndDumbPipes):
   no central orchestrator for business or technical processes, no broker that has to understand your domain. This makes
   it easy for the utilized technology to scale.
4. **Have one write destination for each operation result** between databases and message brokers, so there is no
   problem coordinating writes between multiple destinations and necessity to span transactions across them.
5. **Make compromises visible for devs.** Where a guarantee is traded away, developers are informed explicitly 
   and are given the tools to compensate. See [Compromises](compromises.md).
6. **Produced data must be durable.** An event written to the store survives anything short of losing the store itself;
   Events must be delivered and processed by consumers at least once.
7. **Be ready for a failure.** Software and hardware may break mid-process. Make every process recoverable. After a
   failure a retry should happen and finish the job without leaving the system in inconsistent state.
8. **Simple is secure.** Design simple solutions. Utilize few dependencies.
9. **Modules should be substitutable.** Every integration point (`EventStore`, `EventProducer`,
   `SnapshotStore`, (de)serializers, the polling transport, the events publisher) is an interface, implementations of
   which could vary.
10. **Developer experience is important.** Events-caravan provides simple interfaces, no DSL, which help devs to stay
    focused on building domain behavior; framework has no complex mechanisms that require configuring, and it fails fast
    when misconfigured.
