# When to choose Events-caravan over Axon

Events-caravan and Axon solve the same problem, which is event sourcing and CQRS, but with opposite approach on
centralization, driven by [Compromise #1](compromises.md#1-no-global-sequence-of-events):

- **Choose Events-caravan** when horizontal scalability without a central writer component is the priority: there is no global
  events sequence and duty to maintain it. Library's footprint is a handful of interfaces over your own database and
  broker, which suits teams already operating DynamoDB/SNS/SQS-shaped infrastructure, or willing to implement the
  equivalent adapters; and comfortable maintaining their own sorted query-model or global event-log, if they actually
  need one (see [Compromise #1](compromises.md#1-no-global-sequence-of-events)).
- **Choose Axon** when a global, replayable stream of all events is needed out of the box. For example rebuilding many
  projections from scratch across every entity, without building that yourself, or when Axon Server's built-in tracking
  processors, deadline manager, and Sagas orchestration outweigh the cost of running and scaling a central server.

**In short:** Events-caravan trades Axon's built-in global event-store and orchestration machinery for horizontal
scalability with fewer moving parts; pick whichever side of that trade fits your team's operational scale and appetite
for infrastructure.
