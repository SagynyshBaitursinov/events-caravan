# AWS reference implementation

Adapters for AWS DynamoDB, SNS/SQS are shipped as the reference implementation of Events-caravan's
technology-agnostic core.

- [Why AWS, and why it suits the project's philosophy](#why-aws-and-why-it-suits-the-projects-philosophy)
- [What could substitute it](#what-could-substitute-it)
- [Partition slicing of the events table](#partition-slicing-of-the-events-table)
- [Consistent reads](#consistent-reads)
- [Configuration properties](#configuration-properties)
- [Infrastructure](#infrastructure)

## Why AWS, and why it suits the project's philosophy

DynamoDB and SNS/SQS were chosen because each maps directly onto a philosophy principle.

Services that are utilized by events-caravan are serverless, and horizontal scaling is provided by AWS itself. This is
the primary reason why these technologies were chosen. Events-caravan utilizes them without constraining the underlying
infrastructure from scaling.

- **DynamoDB** partitions natively by primary key, has no single leader node applications write through, and scales
  throughput per partition rather than globally, directly
  serving [Principle #1 (enable horizontal scaling)](philosophy.md). DynamoDB Streams give an ordered, per-shard
  change feed for free, which is what lets an event item insert *be equal* to the publishing it
  (see [Producing and propagating events](architecture.md#producing-and-propagating-events) point 4) without a
  transactional outbox, serving [Principle #4 (one write destination)](philosophy.md).
- **SNS/SQS** is a broker that does not need to understand the domain: SNS fans out by opaque message attributes, and
  SQS queues are owned and scaled independently by each consumer application. This is "dumb pipes"
  per [Principle #3](philosophy.md), there is no central bus or orchestrator coordinating consumers.
- **The publisher Lambda** only moves records from DynamoDB Streams to SNS; it holds no business logic and scales with
  the number of stream shards, so it does not become a bottleneck or a single point of failure, consistent with
  [Principle #1](philosophy.md).

None of this is AWS-specific in substance, only in the concrete API used, and
per [Principle #9](philosophy.md) every integration point is an interface open for substitution
(`EventStore`, `EventProducer`, `SnapshotStore`, event consumption, the publisher).

> [!NOTE]
> When operating applications on a high scale it might be worth switching to provisioned capacity of DynamoDB, 
> with reserved instances. However, if very high-scale load is expected, it might be worthy to plan utilizing different
> technologies not to pay AWS per request, but operate your own DB and Message broker. On the other hand that might
> increase operational cost of the system.

## What could substitute it

Technologies with the following properties could substitute the reference implementation:

- A database that can reject a duplicate `(entityReference, sequenceNumber)` on insert, read one entity's events in
  order, and expose its inserts as a change feed can substitute DynamoDB. How far writes scale then depends on that
  database.
- A broker letting consumers own and scale their own subscriptions without a central process understanding the domain
  can substitute SNS/SQS: for example Kafka, Pulsar, or Google Pub/Sub.
- The publisher can be substituted by a stream-reader process as long as it preserves at-least-once delivery and keeps
  the insert as the sole trigger for publishing.

## Partition slicing of the events table

"Hot" entities with long histories can make a partition and database shard where its events are stored too heavy. 
In the DynamoDB adapter partition-key is `entityName#entityId#sliceIndex` where
one `sliceIndex` increments every N events defined by `partition-slice-size` property. Slice indexes are incremental,
and sort-key is `sequenceNumber`, so all events are sorted in each sliced partition. Because `sliceIndex` is derived
from `sequenceNumber` and `partition-slice-size`, `partition-slice-size` parameter must stay fixed for the lifetime
of the table.

## Consistent reads

Per [Compromise #4](compromises.md#4-loaded-state-may-be-stale), a loaded entity's state may be stale.
With the DynamoDB adapter `consistent-read` could be used to reduce loading stale entity states, which can be enabled via
[properties](#configuration-properties). Eventually consistent reads cost half as much, 
so avoid it unless frequent parallel writes to the same entities are anticipated.

## Configuration properties

```yaml
caravan:
  event:
    sourcing:
      event-store:
        dynamo-db:
          table-name: my-app_events
          partition-slice-size: 10000   # events per partition-key slice
          query-max-page-size: 1000     # events loaded into memory per page while replaying an entity
          consistent-read: false        # opt into strongly consistent reads if needed
      snapshot-store:
        dynamo-db:
          table-name: my-app_snapshots
          consistent-read: false      # opt into strongly consistent reads if needed
      entity-stream:
        dynamo-db:
          table-name: my-app_entity-stream   # absent by default: entity stream is off unless this is set
    messaging: # only with the SQS starter
      queue-name-prefix: my-app       # queues are named {prefix}_{entityName}
      subscribed-entities:
        - calculator
      graceful-shutdown-seconds: 10   # set 0 for immediate shutdown
      concurrency: 10                 # max in-flight messages per queue
      max-poll-size: 10               # max messages requested per poll
      min-poll-size: 3                # min free capacity worth polling for
      pollers-count-cap: 0            # max poller threads per queue; 0 = derived from concurrency and max-poll-size
      poll-wait-seconds: 10           # long-poll wait per request
      deletion: # batching of consumed-message deletions
        max-batch-size: 10
        period-seconds: 1
        concurrency: 3
```

> [!NOTE]
> All values except the table names, `queue-name-prefix` and `subscribed-entities` properties are shown at their
> defaults and can be omitted. `entity-stream.dynamo-db.table-name` has no default: it is what turns the optional
> [Entity-stream](guide/05-entity-stream.md) on.

> [!IMPORTANT]
>`event-store.dynamo-db.partition-slice-size` is baked into how items are keyed and must be fixed once the events
> table is populated. See [Partition slicing](#partition-slicing-of-the-events-table).

## Infrastructure

Events-caravan provides no infrastructure, and is infrastructure-agnostic. An environment deploying the reference
implementation based on AWS must provide: the events and snapshots DynamoDB tables, plus the entity-stream DynamoDB
table if [Entity-Stream](guide/05-entity-stream.md) is enabled. The events table should have
`NEW_IMAGE` stream enabled, for which a `Node.js` based Lambda code is provided, which can be used as a reference for
publishing events. It's used for integration-tests as well. The Lambda code itself is documented in
the [Lambda's README](../dynamodb-sqs-events-publisher-lambda/README.md).

> [!TIP]
> For local development, `./local/env-up` provisions the entire pipeline (tables, stream, Lambda, topics, queues)
> against a local AWS simulator run by Docker Compose; and `./local/execute-tests` runs the full integration test suite against
> it.
