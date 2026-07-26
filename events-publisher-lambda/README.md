# events-publisher-lambda

Forwards events from the event store to SNS.

Events are written to a DynamoDB table by
`io.saga.caravan.event.sourcing.dynamodb.DynamoDbBasedEventStore` and are immutable, so every
insert into that table is exactly one new event. This lambda is subscribed to the table's stream and
republishes each inserted item to the SNS topic of its entity, where consumers pick it up.

This repository holds the lambda code only (`index.mjs`). The infrastructure it runs on is not
provisioned here — the [Infrastructure expectations](#infrastructure-expectations) below are what
the code assumes to be in place.

## Contents

- [What the lambda does](#what-the-lambda-does)
- [Environment variables](#environment-variables)
- [Input: DynamoDB stream records](#input-dynamodb-stream-records)
- [Output: SNS messages](#output-sns-messages)
- [Batching and parallelism](#batching-and-parallelism)
- [Delivery semantics](#delivery-semantics)
- [Failure handling](#failure-handling)
- [Infrastructure expectations](#infrastructure-expectations)
- [Local development](#local-development)
- [What the lambda deliberately does not do](#what-the-lambda-deliberately-does-not-do)

## What the lambda does

For each invocation:

1. Take the `INSERT` records of the DynamoDB stream event and read their `NewImage`.
2. Split the item's partition key back into `entityName` and `entityId`.
3. Build the SNS message body and its message attributes.
4. Group the messages by target topic into batches that fit an SNS `PublishBatch` request.
5. Publish those batches concurrently.
6. Report the earliest record that was not published back to the event source mapping, so the stream
   is rewound to it.

## Environment variables

| Name                | Required | Set by | Purpose                                                                                                                                        |
|---------------------|----------|--------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `APP_NAME`          | yes      | you    | Prefix of every topic name (`${APP_NAME}_${entityName}`). The lambda refuses to initialize without it, so a missing value fails at cold start rather than per record. |
| `AWS_ENDPOINT_URL`  | no       | you    | Overrides the AWS endpoint, for local simulators. Read by the AWS SDK itself — the lambda contains no code for it.                              |
| `AWS_REGION`        | yes      | Lambda | Part of the topic ARN. Set by the Lambda runtime; it is a reserved variable and cannot be configured by hand on real AWS.                        |
| `AWS_LAMBDA_*`, credentials | yes | Lambda | Standard runtime and credential variables consumed by the SDK.                                                                        |

Nothing else is needed. Partition, region and account id come from `context.invokedFunctionArn`, and
the entity name comes from the record itself, so no topic ARN, account id or region has to be
configured.

## Input: DynamoDB stream records

The lambda is driven by the stream of the events table, whose items
`DynamoDbBasedEventStore.mapEventToAttributes` writes as:

| Attribute         | Type | Content                                                                      |
|-------------------|------|------------------------------------------------------------------------------|
| `entityReference` | `S`  | Partition key, `entityName#entityId#shardIndex`                              |
| `sequenceNumber`  | `N`  | Sort key, gapless and starting at 1 per entity                               |
| `eventName`       | `S`  | Domain event name                                                            |
| `timestamp`       | `S`  | ISO-8601, UTC, truncated to milliseconds                                     |
| `payload`         | `S`  | The event payload, already serialized to JSON by the producer                |

`shardIndex` is the event store's hot-partition sharding (`sequenceNumber` divided by the configured
shard size) and is dropped here — it is a storage concern, not part of the entity's identity.
`EntityReferenceKeyUtils` rejects `#` inside `entityName` and `entityId`, so the key always splits
into exactly three parts; anything else is treated as corruption and rewound (see
[Failure handling](#failure-handling)).

`record.eventName` on the stream record is the DynamoDB operation (`INSERT`), not the domain
`eventName` attribute. The event source mapping is expected to filter on `INSERT`; the lambda
re-checks it so that a missing filter cannot turn into republished events.

## Output: SNS messages

### Topic

```
arn:${partition}:sns:${region}:${accountId}:${APP_NAME}_${entityName}
```

`partition`, `region` and `accountId` are taken from `context.invokedFunctionArn`, so the lambda
publishes into its own account and region and needs no configuration to do so. The prefix is derived
once per execution environment and cached.

### Message body

The body is the JSON that `io.saga.caravan.event.serialization.jackson.JacksonEventDeserializer`
expects, mirroring `io.saga.caravan.event.Event`:

```json
{
  "entityReference": {"entityName": "calculator", "entityId": "42"},
  "eventName": "ValueAdded",
  "sequenceNumber": 7,
  "timestamp": "2026-07-26T10:15:30.123Z",
  "payload": {"value": 5}
}
```

`payload` is embedded as raw JSON, not as a JSON string. It is stored in DynamoDB exactly as the
producer serialized it, so the lambda splices it into the body verbatim instead of parsing and
re-serializing it: that saves CPU on every event and rules out the deserializer seeing a payload
that changed in transit (number precision, property order).

### Message attributes

| Attribute    | Type     | Value                                |
|--------------|----------|--------------------------------------|
| `entityName` | `String` | Entity name from the partition key   |
| `eventName`  | `String` | Domain event name                    |

They exist so subscriptions can use SNS filter policies, letting AWS drop messages a consumer does
not want before they reach a queue.

## Batching and parallelism

Messages are grouped by topic and published with `PublishBatch`, up to 10 entries per request. SNS
counts its 256 KB limit per request rather than per message, so a batch is also closed once its
measured size would exceed that. Grouping ignores stream order, so all events of one topic in an
invocation go into as few requests as possible.

Batches are published concurrently through a bounded worker pool (10 in flight), so an invocation
covering many entities is not serialized behind one topic and cannot open an unbounded number of
connections to SNS either.

A batch that ends up holding a single entry is sent with `Publish` instead of `PublishBatch` — this
is the "too large to batch" case, and publishing it on its own drops the batch envelope that would
otherwise push it further over the limit. An entry that is too large even alone fails, and is
handled as any other failure below.

## Delivery semantics

- **At least once.** A rewind republishes everything from the rewind point on, including messages
  that already reached SNS. Consumers must be idempotent — `entityReference` plus `sequenceNumber`
  identifies an event uniquely, and `Event.equals` is defined on exactly those two fields.
- **No ordering guarantee.** Standard topics and standard queues do not preserve order, and batching
  reorders within an invocation as well. Consumers that care about order must use `sequenceNumber`,
  which is gapless and starts at 1 per entity.
- **One topic per entity name**, not per entity instance.

## Failure handling

The lambda never gives up on a record silently and never writes to a DLQ itself. It tracks which
entries reached SNS and returns the earliest record that did not:

```json
{"batchItemFailures": [{"itemIdentifier": "<stream sequence number>"}]}
```

That requires `ReportBatchItemFailures` on the event source mapping. The cases:

| Situation                                            | Lambda                                              | AWS                                                  |
|------------------------------------------------------|-----------------------------------------------------|------------------------------------------------------|
| Whole invocation succeeded                            | Returns an empty `batchItemFailures`                | Checkpoint advances                                  |
| Some batches failed or were rejected                  | Rewinds to the earliest unpublished record          | Redelivers from there                                |
| A record cannot be turned into a message (corrupt item, missing attribute) | Stops preparing, rewinds to that record | Redelivers from there                                |
| A record keeps failing (too large, permanently rejected) | Rewinds to it on every attempt                   | Exhausts `MaximumRetryAttempts` / `MaximumRecordAgeInSeconds`, then sends failure metadata to the on-failure destination and moves past it |

Because grouping ignores stream order, the earliest unpublished record is not necessarily one that
failed — entries of another topic may sit before it and never have been attempted. Rewinding to the
earliest one is what keeps the stream gapless, at the cost of the duplicates described above.

Note that the on-failure destination receives **metadata** about the failed invocation (stream ARN,
shard id, sequence number range, error), not the event bodies. Recovering the events themselves
means reading them back out of the events table with that metadata.

## Infrastructure expectations

None of this is created by this repository. It is what the code assumes.

### Function

| Setting | Expected value                                                              |
|---------|-------------------------------------------------------------------------------|
| Runtime | `nodejs24.x`                                                                  |
| Handler | `index.handler`                                                               |
| Package | `lambda.zip`, containing `index.mjs` only (see [`zip`](./zip))                 |
| Memory  | 512 MB is enough; the lambda holds one stream batch plus its messages in memory |
| Timeout | At least 10 s. Worst case is roughly `ceil(batches / 10) × 3 s`, from the SDK's 3 s request timeout and the pool of 10 |

The package carries no dependencies and expects `@aws-sdk/client-sns` from the runtime. Every
managed Node.js runtime so far has shipped the AWS SDK for JavaScript v3, but AWS recommends
bundling it for version stability — if `nodejs24.x` does not provide it, `zip` has to package
`node_modules` as well.

### Events table

- Stream enabled with `StreamViewType` of `NEW_IMAGE` (or `NEW_AND_OLD_IMAGES`); the lambda only
  reads `NewImage`.
- Partition key `entityReference` (`S`), sort key `sequenceNumber` (`N`), as written by
  `DynamoDbBasedEventStore`.

### Event source mapping

| Setting                            | Expected value                                                        |
|------------------------------------|-------------------------------------------------------------------------|
| `EventSourceArn`                   | The events table's `LatestStreamArn`                                    |
| `StartingPosition`                 | `LATEST` for a new deployment, `TRIM_HORIZON` to replay                 |
| `FilterCriteria`                   | `{"Filters":[{"Pattern":"{\"eventName\":[\"INSERT\"]}"}]}` — AWS drops everything else before the lambda is even invoked, so nothing is paid for records that are not events |
| `FunctionResponseTypes`            | `ReportBatchItemFailures` — required, otherwise a single failure replays the whole batch |
| `BatchSize`                        | 100–1000. Batching only pays off with enough records per invocation; the SNS batch of 10 caps how much one record can cost |
| `MaximumBatchingWindowInSeconds`   | 1–5. Trades a little latency for fuller batches and fewer invocations    |
| `ParallelizationFactor`            | Up to 10, to get more concurrency out of one shard                       |
| `MaximumRetryAttempts`             | Bounded (e.g. 10). Without a bound, one poison record blocks its shard forever |
| `MaximumRecordAgeInSeconds`        | Bounded as a second guard against a stuck shard                          |
| `DestinationConfig.OnFailure`      | An SQS queue, SNS topic or S3 bucket — this is the DLQ. Without it, exhausted records are dropped silently |
| `BisectBatchOnFunctionError`       | `true`. Does not apply to `ReportBatchItemFailures` responses, but isolates the record behind a hard failure such as an out-of-memory kill |

### Topics

One standard topic per entity name, named `${APP_NAME}_${entityName}`, created before the first
event of that entity is written. The lambda does not create topics: publishing to a topic that does
not exist fails, and those events stall — and eventually reach the DLQ — until it is created.

Subscribers that want the message body as published should set `RawMessageDelivery` on their
subscription; without it SNS wraps the body in its own envelope and `JacksonEventDeserializer`
cannot read it.

### Execution role

| Action                                                                                        | Resource                                          |
|-----------------------------------------------------------------------------------------------|---------------------------------------------------|
| `dynamodb:DescribeStream`, `dynamodb:GetRecords`, `dynamodb:GetShardIterator`, `dynamodb:ListStreams` | The events table's stream ARN               |
| `sns:Publish`                                                                                   | `arn:${partition}:sns:${region}:${accountId}:${APP_NAME}_*` |
| `sqs:SendMessage` / `sns:Publish` / `s3:PutObject`                                              | The on-failure destination                        |
| `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`                              | The function's log group                          |

## Local development

[`local/env-up`](../local/env-up) brings up a floci container and provisions the table, stream,
topics, queues, function and event source mapping against it. [`zip`](./zip) packages `index.mjs`
into `lambda.zip` and is sourced by `env-up`.

Two settings in `local/env-up` still have to be brought in line with this lambda:

- the runtime is `nodejs20.x` and has to become `nodejs24.x`;
- the endpoint is passed as `AWS_ENDPOINT_OVERRIDE` and has to become `AWS_ENDPOINT_URL`.

Its `AWS_ACCOUNT_ID` and `AWS_REGION` variables are not read by the lambda — the account comes from
the invoked function ARN and the region from the runtime — and `AWS_REGION` cannot be set this way
on real AWS at all.

## What the lambda deliberately does not do

Each of these is left to AWS, so that there is less code to run, pay for and get wrong:

- **No DLQ handling.** Poison records are reported back to the stream and moved to the on-failure
  destination by the event source mapping.
- **No retry loop.** A failed publish is a rewind; redelivery, backoff and the retry ceiling belong
  to the event source mapping.
- **No filtering of non-`INSERT` records** beyond a cheap guard — `FilterCriteria` does it before
  invocation.
- **No deduplication and no ordering.** `sequenceNumber` gives consumers what they need for both.
- **No topic management.** Topics are provisioned with the rest of the infrastructure.
- **No payload parsing.** The payload is copied through as the bytes the producer wrote.
