# Events Caravan

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
![Java](https://img.shields.io/badge/Java-25-ED8B00)
![Spring Boot](https://img.shields.io/badge/Spring_Boot-4.x-6DB33F)
![Reference impl](https://img.shields.io/badge/reference_impl-DynamoDB_·_SNS%2FSQS-FF9900)
[![Maven Central](https://img.shields.io/maven-central/v/dev.baitursinov/events-caravan-core.svg)](https://central.sonatype.com/artifact/dev.baitursinov/events-caravan-core)

Event-sourcing framework with no global events sequence, no locks and no central coordinator, so it never prevents its 
database's writer nodes from scaling horizontally. The core is technology-agnostic and every integration point 
is an interface that can be implemented for any database and message broker fitting the framework's philosophy and contracts. 
Adapters for [AWS DynamoDB, SNS/SQS](docs/guide/01-modules-and-dependencies.md) are shipped as a
[reference implementation](docs/aws-reference-implementation.md).

## Principles at a glance

1. **Enable horizontal scaling**: nothing global, no global events sequence, locks or coordinator.
2. **Be satisfied with Eventual Consistency**: every read and write is keyed by one entity.
3. **Utilize simple and cheap technology features**: smart endpoints and dumb pipes.
4. **Have one write destination for each operation result**: no dual writes, no transactions across destinations.
5. **Make compromises visible for devs**: see [Trade-offs you accept](#trade-offs-you-accept).
6. **Produced data must be durable**: events are delivered and processed at least once.
7. **Be ready for a failure**: every process is recoverable by a retry.
8. **Simple is secure**: simple solutions, few dependencies.
9. **Modules should be substitutable**: every integration point is an interface.
10. **Developer experience is important**: simple interfaces, no DSL, fail fast when misconfigured.

Read the full [Philosophy in principles](docs/philosophy.md).

## Trade-offs you accept

1. [No global sequence of events](docs/compromises.md#1-no-global-sequence-of-events), compensated by an optional
   [Entity-stream](docs/guide/05-entity-stream.md).
2. [At-least-once, unordered delivery](docs/compromises.md#2-at-least-once-unordered-delivery): handlers must be idempotent.
3. [No cross-entity transactions are required](docs/compromises.md#3-strong-consistency-across-entities-is-not-required-from-adapters):  use Sagas where you need consistency
4. [Loaded state may be stale](docs/compromises.md#4-loaded-state-may-be-stale): be aware and retry on `DuplicateEventProductionException`.

## Quick start

Add starters:

```xml
<dependency>
    <groupId>dev.baitursinov</groupId>
    <artifactId>events-caravan-dynamodb-spring-boot-starter</artifactId>
    <version>${events-caravan-version}</version>
</dependency>
<dependency>
    <groupId>dev.baitursinov</groupId>
    <artifactId>events-caravan-sqs-spring-boot-starter</artifactId>
    <version>${events-caravan-version}</version>
</dependency>
```

Register events, define an entity, its repository, event handlers:

```java
@Bean
public EntityEventsRegistration calculatorEventsRegistration() {
  return new EntityEventsRegistration(
      "calculator", 
      Map.of("number-added", NumberCarryingPayload.class));
}

@EntityName("calculator")
public class Calculator extends EventSourcedEntity {

  private final String id;
  long currentNumber = 0;

  public Calculator(String id) { this.id = id; }

  public void addNumber(long number) {
    recordEvent("number-added", new NumberCarryingPayload(number));
  }

  @ApplyEvent("number-added")
  private void applyAddNumber(Event<NumberCarryingPayload> event) {
    this.currentNumber += event.payload().number();
  }

  @Override
  public String entityId() { return id; }
}

@Repository
public class CalculatorRepository extends EventSourcedRepository<Calculator> {

  public CalculatorRepository(EventSourcingRepositoryContext context) {
    super(Calculator.class, context);
  }

  @Override
  protected Calculator createWithBlankState(String entityId) { return new Calculator(entityId); }
}
```

Use it:

```java
var calculator = new Calculator("42");
calculator.addNumber(10);
calculatorRepository.save(calculator);

var found = calculatorRepository.findBy("42").orElseThrow();
```

Subscribe to events:
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

Follow the [User guide](#documentation) for modules, external event appliers, snapshots, event handlers, serialization and entity stream.

## Documentation

- [Philosophy in principles](docs/philosophy.md)
- [Design and Architecture](docs/architecture.md): event structure, storage, propagation, consumption, optimistic
  concurrency control, CQRS
- [Compromises](docs/compromises.md)
- User guide
    1. [Modules and dependencies](docs/guide/01-modules-and-dependencies.md)
    2. [Events and entities](docs/guide/02-events-and-entities.md)
    3. [Repositories and snapshots](docs/guide/03-repositories-and-snapshots.md)
    4. [Event handlers](docs/guide/04-event-handlers.md)
    5. [Entity-stream](docs/guide/05-entity-stream.md)
- [AWS reference implementation](docs/aws-reference-implementation.md): why AWS, substitutes, configuration
  properties, infrastructure
- [When to choose Events-caravan over Axon](docs/comparison-axon.md)
- [FAQ](docs/faq.md)
- [Roadmap](ROADMAP.md)

## Contributing

Events-caravan is created and currently maintained by [Sagynysh Baitursinov](https://github.com/SagynyshBaitursinov).
Bug reports, feature proposals, and pull requests are welcome — see the
[contributing guide](CONTRIBUTING.md) for the development setup and workflow, and the
[code of conduct](CODE_OF_CONDUCT.md) for community standards. Security vulnerabilities should be reported privately as
described in the [security policy](SECURITY.md).

## Further reading

- [Fowler on Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html)
- [Dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Tyler Treat's "You Cannot Have Exactly-Once Delivery"](https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/)
- [Gregor Hohpe's "Your Coffee Shop Doesn't Use Two-Phase Commit"](https://www.enterpriseintegrationpatterns.com/docs/IEEE_Software_Design_2PC.pdf)

## License

[Apache License 2.0](LICENSE)
