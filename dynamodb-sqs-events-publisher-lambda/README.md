# dynamodb-sqs-events-publisher-lambda

Lambda publishes events from the event-store DynamoDB Stream into SNS. This is a module of the library, which can be
replaced by another solution, if it does not fit the infrastructure. The module holds the lambda code only
(`index.mjs`). The infrastructure it runs is not provisioned here,
but [Infrastructure expectations](#infrastructure-expectations) below describe what the code assumes to be in place.

## What the lambda does

Events are written to a DynamoDB table by
`io.saga.caravan.event.sourcing.dynamodb.DynamoDbBasedEventStore` and are immutable, so every insert into that table is
exactly one new event. This lambda is subscribed to the table's stream and republishes each inserted record to the SNS
topic corresponding to its entity, where consumers can pick it up.

For each invocation the lambda does the following:

1. Take the `INSERT` records of the stream event and read their `NewImage`.
2. Derives topicName based on APP_NAME and entityName.
3. Build the SNS message body and its message attributes.
4. Group the messages by target topic into `PublishBatch` requests of up to 10 entries.
5. Publish the batches, at most `MAX_CONCURRENT_PUBLISHES` in flight at a time.
6. Report the earliest stream record that was not published back as `ReportBatchItemFailures`; it rewinds the stream
   iterator to that record and re-invokes the lambda from there.

## Environment variables

| Name                       | Required | Purpose                                                                                                                          |
|----------------------------|----------|----------------------------------------------------------------------------------------------------------------------------------|
| `APP_NAME`                 | yes      | Prefix of every topic name (`${APP_NAME}_${entityName}`). A missing value fails at cold start rather than per record.            |
| `MAX_CONCURRENT_PUBLISHES` | no       | Caps how many `PublishBatch`/`Publish` calls are in flight at once per invocation. Defaults to `10`. Must be a positive integer. |
| `AWS_ENDPOINT_URL`         | no       | Overrides the AWS endpoint, for local simulators. Read natively by the AWS SDK — the lambda contains no code for it.             |

Nothing else is configured. Partition, region and account id come from
`context.invokedFunctionArn`, and the entity name comes from the record itself, so no topic ARN, account id or region
ever has to be wired in.

## Input: DynamoDB stream records

The stream must carry the new image of each record (`StreamViewType=NEW_IMAGE`). Records are written by
`DynamoDbBasedEventStore.mapEventToAttributes` as:

| Attribute         | Type | Content                                                          |
|-------------------|------|------------------------------------------------------------------|
| `entityReference` | `S`  | Partition key, `entityName#entityId#shardIndex`                  |
| `sequenceNumber`  | `N`  | Sort key, gapless and starting at 1 per entity                   |
| `eventName`       | `S`  | Name of the event                                                |
| `timestamp`       | `S`  | ISO-8601 instant in UTC, always `uuuu-MM-dd'T'HH:mm:ss.SSS'Z'`   |
| `payload`         | `S`  | The event payload, already serialized to JSON by the event store |

The table is insert-only by design, and the event source mapping should filter on stream `eventName: INSERT` (see
below). Records of any other type are skipped anyway.

## Output: SNS messages

The target topic ARN is derived per record:

```
arn:{partition}:sns:{region}:{accountId}:{APP_NAME}_{entityName}
```

with partition, region and account id taken from `context.invokedFunctionArn`.

The message body mirrors `io.saga.caravan.event.Event`:

```json
{
  "entityReference": {
    "entityName": "calculator",
    "entityId": "42"
  },
  "eventName": "NumberAdded",
  "timestamp": "2026-07-26T10:15:30.123Z",
  "sequenceNumber": 17,
  "payload": { "example-domain-field" : "example-domain-value" }
}
```

`payload` is spliced into the body as the raw JSON string stored in the record — the lambda never parses or
re-serializes it.

Every message carries two String message attributes, `entityName` and `eventName`, so consumers can narrow their
subscriptions with SNS filter policies instead of discarding messages after delivery.

## Batching and concurrency

Messages are grouped by target topic into `PublishBatch` requests of up to 10 entries (the SNS maximum). Batches are
sent in parallel — but every actual SNS call passes through a concurrency limiter first, so at most
`MAX_CONCURRENT_PUBLISHES` calls are ever in flight. A large invocation (e.g. a big `--batch-size`) therefore cannot fan
out into multitude of simultaneous SNS calls to hit AWS throttling. Order between batches is deliberately not
preserved — a standard SNS topic would not preserve it anyway, so serializing the requests would cost latency and buy
nothing applications could rely on.

The lambda does no byte counting. SNS enforces its size limit itself, and the lambda reacts to its errors instead of
predicting them: when a batch is rejected as too large (`BatchRequestTooLongException`), its entries are republished 1
by 1 with plain `Publish`
calls, in parallel. These fallback calls share the same limiter as `PublishBatch`, so they cannot push total in-flight
SNS calls past `MAX_CONCURRENT_PUBLISHES` either. An individual entry that still fails then is reported as failed.

## Delivery semantics

**At-least-once, unordered.** Consumers must be idempotent and must tolerate events arriving out of order. Duplicates
come from two directions:

- SNS standard topics deliver at-least-once by themselves.
- On a partial failure the stream iterator rewinds to the earliest unpublished record, so records after it that were
  already published get republished.

Consumers that need order or exact-once processing may utilize unique `entityReference` + `sequenceNumber` combination —
the sequence is gapless and starts at 1 per entity. Otherwise, applications should apply their own logic to achieve
idempotence and correct ordering, if needed.

## Failure handling

The handler never throws for record-level problems. Every failure is reported through
`ReportBatchItemFailures` as the single earliest unpublished stream sequence number — a stream source only honors the
lowest reported identifier anyway — and the retry mechanism does the rest.

- **Malformed record** (unparseable key, missing attributes): that record and everything after it in the invocation is
  reported as unpublished.
- **Any publish failure** — throttling, 5xx, missing topic, message too large: the earliest stream sequence number that
  has failed is reported as unpublished.
- **Misconfiguration** (missing `APP_NAME`, or an invalid `MAX_CONCURRENT_PUBLISHES`): the only things that throw, at
  cold start. That is a deployment bug, and crashing the invocation is the correct signal.

A record that can never be published — a message over the SNS size limit, a topic that does not exist — is redelivered
until the mapping's `MaximumRetryAttempts` is exhausted, then handed to the `OnFailure` destination. Until then, it
blocks its stream shard, which is why a finite `MaximumRetryAttempts` is required.

## Infrastructure expectations

This module ships no infrastructure code. Whoever deploys the lambda provides:

1. **The events table** with a stream, `StreamViewType=NEW_IMAGE`.
2. **One SNS topic per entity type**, named `${APP_NAME}_${entityName}`, created **before** the first event of that
   entity type is produced. Publishing to a missing topic fails and, after the retries run out, lands in the failure
   destination.
3. **The lambda** on the `nodejs24.x` runtime, handler `index.handler`, with the `APP_NAME`
   environment variable. Its `--timeout` must cover publishing a whole invocation taking into account --batch-size,
   MAX_CONCURRENT_PUBLISHES, AWS SDK timeouts and internal retries. A lambda timeout hit mid-invocation fails the whole
   batch and has it redelivered, so keep it generous;
4. **The event source mapping** from the table's stream to the lambda:
    - `--function-response-types ReportBatchItemFailures` — **required**.
    - `--filter-criteria '{"Filters": [{"Pattern": "{\"eventName\": [\"INSERT\"]}"}]}'` — recommended, so non-insert
      records never cost an invocation.
    - A finite `MaximumRetryAttempts` — **required**, or a poisonous record blocks its shard until the record ages out
      of the stream (24 h). It also bounds how long a transient SNS outage may last before records divert to the failure
      destination, since retries back off over minutes — so do not set it too low either.
    - An `OnFailure` destination (an SQS queue is the usual choice) — **required**, see
      [the next section](#handling-the-failure-destination).
    - Optional tuning: a larger `--batch-size` amortizes invocations and lets batching by topic pay off, though may
      amplify the number of duplicate deliveries in case of failures; a positive `MaximumBatchingWindowInSeconds`
      trades latency for fewer invocations at low traffic (leave it at 0 when latency matters — under load the stream
      hands over accumulated records in bulk regardless); `ParallelizationFactor` adds concurrency per shard, which
      multiplied by `MAX_CONCURRENT_PUBLISHES` makes the max number of simultaneous SNS calls in a stream shard.
5. **IAM** for the lambda role: `sns:Publish` on `${APP_NAME}_*` topics; `dynamodb:GetRecords`,
   `dynamodb:GetShardIterator`, `dynamodb:DescribeStream`, `dynamodb:ListStreams` on the stream;
   `sqs:SendMessage` on the failure queue; CloudWatch Logs; `kms:Decrypt` and `kms:GenerateDataKey*`, if topics use SSE
   with a customer-managed key.

## Handling the failure destination

For stream sources, the `OnFailure` destination receives **metadata only** — the stream ARN, the shard id, the *stream*
sequence-number range of the failed batch, approximate arrival timestamps and the error — never the events themselves.
An entry in the failure queue is a pointer to location in shard stream, not a replayable event message, and the pointer
has a shelf life:

- **Within the stream's 24h retention** it is fully resolvable: `GetShardIterator`
  (`AT_SEQUENCE_NUMBER` at the range's start) and `GetRecords` on the events table's stream read the exact failed
  records back, `NewImage` included, ready to republish.
- **After 24 h** the stream records expire and the pointer dangles — stream sequence numbers cannot be mapped to table
  keys. The events themselves are still safe in the immutable event store, but stream entries identifying which Events
  were not to delivered to SNS disappear.

How those entries are handled is up to the surrounding infrastructure and to what its consumers need; the library does
not prescribe an exact mechanism. Recommendation is that failures should be handled or  
corresponding events references should be saved within 24h, before stream entries disappear.

## Local development

`local/env-up` provisions the whole pipeline against a local AWS simulator: tables, topics, queues, the lambda (zipped
from `index.mjs` by `/zip`) and the event source mapping, with `AWS_ENDPOINT_URL` pointing at the simulator.

## What the lambda deliberately does not do

- **No byte counting** — SNS enforces its own limits; the lambda reacts to errors.
- **No application-level retries and no dead-lettering** — beyond the AWS SDK's own default of 3 attempts per request,
  redelivery and DLQ routing belong to the event source mapping.
- **No ordering or (only) one time delivery guarantees** — consumers own responsibility to be idempotent and manage
  ordering, with unique and gapless `entityReference` + `sequenceNumber` available, if they need it.
- **No JSON parsing of payloads** — the stored payload string is passed through verbatim.
- **No logging library** — `console` plus the runtime's structured logging already produce JSON logs with levels, so a
  dependency would only add cold start time.
- **No infrastructure management** — topics, queues, mappings and policies are provisioned outside this module.
