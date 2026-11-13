# Modules and dependencies

| Module                                      | Purpose                                                                                                                                                |
|---------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| events-caravan-core                         | Core, technology-agnostic: entities, repositories, event registry, interfaces, Jackson serialization, event consumption and handlers, entity stream    |
| events-caravan-dynamodb                     | `EventStore` + `EventProducer` + `SnapshotStore` + `EntityStreamWriter` implementations on DynamoDB, with partition slicing for long entity histories |
| events-caravan-queue-polling                | Transport-agnostic continuous polling mechanism (virtual threads, adaptive poller scaling, batched deletes, graceful shutdown)                         |
| events-caravan-sqs                          | SQS polling/deletion primitives                                                                                                                        |
| events-caravan-spring-boot-starter          | Auto-configuration of the core components                                                                                                              |
| events-caravan-dynamodb-spring-boot-starter | Auto-configuration of the DynamoDB event, snapshot and entity stream stores                                                                            |
| events-caravan-sqs-spring-boot-starter      | Auto-configuration of per-entity SQS queue polling                                                                                                     |
| dynamodb-sqs-events-publisher-lambda        | Node.js based Lambda publishing events from DynamoDB's stream into SNS ([its README](../../dynamodb-sqs-events-publisher-lambda/README.md))            |
| events-caravan-integration-tests            | End-to-end tests against a local AWS simulator                                                                                                         |

Add the starters your service needs:

```xml

<dependencies>
    <dependency>
        <groupId>dev.baitursinov</groupId>
        <artifactId>events-caravan-dynamodb-spring-boot-starter</artifactId>
        <version>${events-caravan-version}</version>
    </dependency>

    <!-- only if this service consumes events -->
    <dependency>
        <groupId>dev.baitursinov</groupId>
        <artifactId>events-caravan-sqs-spring-boot-starter</artifactId>
        <version>${events-caravan-version}</version>
    </dependency>
</dependencies>
```

> [!NOTE]
>- If default Jackson based (de)serializers are to be used, the events-caravan's spring-boot-starter does not bring in
   > the `JsonMapper` bean it requires. Your application should configure and provide it. The default Jackson-based
   > serialization activates when Jackson 3 is on the classpath and a `JsonMapper` bean is provided in Spring context.
>
>- Alternatively, provide your own `EventSerializer`, `EventPayloadSerializer`, `EventDeserializer`,
   > `EventPayloadDeserializer`, `SnapshotSerializer` and `SnapshotDeserializer` beans to use a different serialization
   > mechanism.
>
>- `DynamoDbClient` / `SqsClient` beans are expected in the context for their corresponding modules to work.
>
>- Every autoconfigured component is `@ConditionalOnMissingBean`, supply your own bean to override them. Any
   > `EventProducer` bean is transparently wrapped into `ValidatingEventProducer` so all produced events are validated
   > against the `EntityEventsRegistry`.

Configuration properties and infrastructure of the reference implementation are described in
[AWS reference implementation](../aws-reference-implementation.md).

---
Next: [Events and entities](02-events-and-entities.md)
