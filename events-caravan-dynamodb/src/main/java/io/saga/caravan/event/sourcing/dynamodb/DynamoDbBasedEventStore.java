package io.saga.caravan.event.sourcing.dynamodb;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.Event;
import io.saga.caravan.event.EventReference;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.producer.DuplicateEventProductionException;
import io.saga.caravan.event.producer.EventProducer;
import io.saga.caravan.event.producer.EventProductionException;
import io.saga.caravan.event.serialization.EventPayloadDeserializer;
import io.saga.caravan.event.serialization.EventPayloadSerializer;
import io.saga.caravan.event.sourcing.EventStore;
import io.saga.caravan.event.sourcing.EventStoreException;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.saga.caravan.event.sourcing.dynamodb.DynamoDbUtils.requireTableIsActive;
import static io.saga.caravan.event.sourcing.dynamodb.PrimaryKeyUtils.toShardedPartitionKeyValue;
import static io.saga.caravan.utils.TextUtils.hasText;
import static java.util.Objects.requireNonNull;

@Slf4j
public class DynamoDbBasedEventStore implements EventStore, EventProducer {

  private static final String PK = "entityReference";
  private static final String SK = "sequenceNumber";
  private static final String EVENT_NAME_KEY = "eventName";
  private static final String TIMESTAMP_KEY = "timestamp";
  private static final String PAYLOAD_KEY = "payload";
  private static final String NOT_YET_EXISTS_CONDITION_EXPRESSION =
      "attribute_not_exists(#pk) AND attribute_not_exists(#sk)";
  private static final Map<String, String> NOT_YET_EXISTS_EXPRESSION_ATTRIBUTE_NAMES =
      Map.of("#pk", PK, "#sk", SK);
  private static final String CONDITIONAL_CHECK_FAILED_CODE = "ConditionalCheckFailed";
  private static final int MAX_TRANSACTION_WRITE_ITEMS = 100;

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      new DateTimeFormatterBuilder().appendInstant(3).toFormatter();

  private final DynamoDbClient dynamoDbClient;
  private final EventPayloadSerializer eventPayloadSerializer;
  private final EventPayloadDeserializer eventPayloadDeserializer;
  private final String eventsTableName;
  private final int maxPageSize;
  private final long partitionShardSize;

  public DynamoDbBasedEventStore(DynamoDbClient dynamoDbClient,
                                 EventPayloadSerializer eventPayloadSerializer,
                                 EventPayloadDeserializer eventPayloadDeserializer,
                                 String eventsTableName,
                                 int maxPageSize,
                                 long partitionShardSize) {
    requireNonNull(dynamoDbClient);
    requireNonNull(eventPayloadSerializer);
    requireNonNull(eventPayloadDeserializer);

    if (!hasText(eventsTableName)) {
      throw new DynamoDbSetupException("eventsTableName must be set");
    }
    requireTableIsActive(dynamoDbClient, eventsTableName);

    if (partitionShardSize <= 0) {
      throw new DynamoDbSetupException(
          "partitionShardSize must be positive, got %d".formatted(partitionShardSize));
    }

    if (maxPageSize <= 0) {
      throw new DynamoDbSetupException(
          "maxPageSize must be positive, got %d".formatted(maxPageSize));
    }

    this.dynamoDbClient = dynamoDbClient;
    this.eventsTableName = eventsTableName;
    this.maxPageSize = maxPageSize;
    this.partitionShardSize = partitionShardSize;
    this.eventPayloadSerializer = eventPayloadSerializer;
    this.eventPayloadDeserializer = eventPayloadDeserializer;
  }

  @Override
  public void produce(Event<?> event) {
    log.debug("Producing {}", event.eventReference());
    try {
      dynamoDbClient.putItem(
          PutItemRequest.builder()
              .tableName(eventsTableName)
              .item(mapEventToAttributes(event))
              .conditionExpression(NOT_YET_EXISTS_CONDITION_EXPRESSION)
              .expressionAttributeNames(NOT_YET_EXISTS_EXPRESSION_ATTRIBUTE_NAMES)
              .build());
    } catch (ConditionalCheckFailedException exception) {
      log.debug("Duplicate event production attempted for {}", event.eventReference());
      throw duplicateEventProductionException(event);
    } catch (Exception exception) {
      throw new EventProductionException(exception);
    }
    log.debug("Produced {}", event.eventReference());
  }

  @Override
  public void produce(List<Event<?>> events) {
    if (events.isEmpty()) {
      log.debug("Received empty list of events for producing");
      return;
    }

    if (events.size() == 1) {
      produce(events.getFirst());
      return;
    }

    if (events.size() > MAX_TRANSACTION_WRITE_ITEMS) {
      throw new EventProductionException(
          "Cannot produce %d events at once: DynamoDB transactions allow at most %d items"
              .formatted(events.size(), MAX_TRANSACTION_WRITE_ITEMS));
    }

    log.debug("Producing {} events for {} in a transaction",
        events.size(), events.getFirst().entityReference());
    try {
      dynamoDbClient.transactWriteItems(
          TransactWriteItemsRequest.builder()
              .transactItems(
                  events.stream()
                      .map(this::toTransactWriteItem)
                      .toList())
              .build());
    } catch (TransactionCanceledException exception) {
      throw eventProductionException(events, exception);
    } catch (Exception exception) {
      throw new EventProductionException(
          "Failed to produce %d events in a transaction: %s"
              .formatted(events.size(), exception.getMessage()));
    }
    log.debug("Produced {} events for {} in a transaction",
        events.size(), events.getFirst().entityReference());
  }

  private TransactWriteItem toTransactWriteItem(Event<?> event) {
    return TransactWriteItem.builder()
        .put(
            Put.builder()
                .tableName(eventsTableName)
                .item(mapEventToAttributes(event))
                .conditionExpression(NOT_YET_EXISTS_CONDITION_EXPRESSION)
                .expressionAttributeNames(NOT_YET_EXISTS_EXPRESSION_ATTRIBUTE_NAMES)
                .build())
        .build();
  }

  private EventProductionException eventProductionException(List<Event<?>> events,
                                                            TransactionCanceledException exception) {
    var duplicateEvents = duplicateEvents(events, exception.cancellationReasons());
    if (duplicateEvents.size() == 1) {
      return duplicateEventProductionException(duplicateEvents.getFirst());
    }
    if (!duplicateEvents.isEmpty()) {
      return new DuplicateEventProductionException(
          "Events already exist: %s".formatted(
              duplicateEvents.stream()
                  .map(this::eventReferenceDescription)
                  .collect(Collectors.joining(", "))));
    }
    return new EventProductionException(
        "Failed to produce %d events in a transaction: %s"
            .formatted(events.size(), exception.getMessage()));
  }

  private List<Event<?>> duplicateEvents(List<Event<?>> events,
                                         List<CancellationReason> cancellationReasons) {
    var duplicateEvents = new ArrayList<Event<?>>();
    for (int i = 0; i < cancellationReasons.size(); i++) {
      if (CONDITIONAL_CHECK_FAILED_CODE.equals(cancellationReasons.get(i).code())) {
        duplicateEvents.add(events.get(i));
      }
    }
    return duplicateEvents;
  }

  private DuplicateEventProductionException duplicateEventProductionException(Event<?> event) {
    return new DuplicateEventProductionException(
        "Event on %s already exists"
            .formatted(eventReferenceDescription(event)));
  }

  private String eventReferenceDescription(Event<?> event) {
    return "%s with sequenceNumber=%s".formatted(event.entityReference(), event.sequenceNumber());
  }

  private Map<String, AttributeValue> mapEventToAttributes(Event<?> event) {
    var attributes = new HashMap<String, AttributeValue>();
    attributes.put(
        PK,
        AttributeValue.fromS(
            toShardedPartitionKeyValue(
                event.entityReference(),
                shardIndexForSequenceNumber(event.sequenceNumber()))));
    attributes.put(
        SK,
        AttributeValue.fromN(
            String.valueOf(event.sequenceNumber())));
    attributes.put(
        EVENT_NAME_KEY,
        AttributeValue.fromS(
            event.eventName()));
    attributes.put(
        TIMESTAMP_KEY,
        AttributeValue.fromS(
            TIMESTAMP_FORMATTER.format(event.timestamp())));
    attributes.put(
        PAYLOAD_KEY,
        AttributeValue.fromS(
            eventPayloadSerializer.serializePayload(event)));
    return attributes;
  }

  private long shardIndexForSequenceNumber(long sequenceNumber) {
    return (sequenceNumber - 1) / partitionShardSize;
  }

  @Override
  public Stream<Event<?>> getEventsOfEntity(EntityReference entityReference,
                                            long fromSequenceNumberExclusive) {
    if (fromSequenceNumberExclusive < 0) {
      throw new EventStoreException(
          "fromSequenceNumberExclusive must not be negative (sequence numbers start at 1; "
              + "0 means from the beginning), got %d".formatted(fromSequenceNumberExclusive));
    }

    log.debug("Initializing stream of events of {} from sequenceNumber={} exclusive",
        entityReference, fromSequenceNumberExclusive);
    return StreamSupport.stream(
        createSpliterator(entityReference, fromSequenceNumberExclusive), false);
  }

  private DynamoDbEventsSpliterator createSpliterator(EntityReference entityReference,
                                                      long fromSequenceNumberExclusive) {
    long firstShardIndex = shardIndexForSequenceNumber(fromSequenceNumberExclusive + 1);

    Map<String, AttributeValue> firstShardExclusiveStartKey
        = getFirstShardExclusiveStartKey(entityReference, fromSequenceNumberExclusive, firstShardIndex);

    return new DynamoDbEventsSpliterator(
        dynamoDbClient,
        partitionShardSize,
        SK,
        firstShardIndex,
        firstShardExclusiveStartKey,
        shardIndex -> shardQueryBuilder(entityReference, shardIndex),
        attributes ->
            this.mapAttributesToEvent(entityReference, attributes));
  }

  private @Nullable Map<String, AttributeValue> getFirstShardExclusiveStartKey(EntityReference entityReference, long fromSequenceNumberExclusive, long firstShardIndex) {
    boolean resumesExactlyAtShardBoundary = fromSequenceNumberExclusive % partitionShardSize == 0;
    if (resumesExactlyAtShardBoundary) {
      return null;
    } else {
      return Map.of(PK, AttributeValue.fromS(toShardedPartitionKeyValue(entityReference, firstShardIndex)),
          SK, AttributeValue.fromN(String.valueOf(fromSequenceNumberExclusive)));
    }
  }

  private QueryRequest.Builder shardQueryBuilder(EntityReference entityReference,
                                                 long shardIndex) {
    return QueryRequest.builder()
        .tableName(eventsTableName)
        .scanIndexForward(true)
        .keyConditionExpression("#pk = :pkVal")
        .expressionAttributeNames(
            Map.of("#pk", PK))
        .expressionAttributeValues(
            Map.of(
                ":pkVal",
                AttributeValue.fromS(
                    toShardedPartitionKeyValue(entityReference, shardIndex))))
        .limit(maxPageSize);
  }

  private Event<?> mapAttributesToEvent(EntityReference entityReference,
                                        Map<String, AttributeValue> attributeValues) {
    long sequenceNumber = Long.parseLong(attributeValues.get(SK).n());
    String eventName = attributeValues.get(EVENT_NAME_KEY).s();
    return Event.builder()
        .entityReference(entityReference)
        .eventName(eventName)
        .sequenceNumber(sequenceNumber)
        .timestamp(deserializeTimestamp(attributeValues))
        .payload(
            deserializePayload(
                attributeValues,
                entityReference,
                eventName,
                sequenceNumber))
        .build();
  }

  private ZonedDateTime deserializeTimestamp(Map<String, AttributeValue> attributeValues) {
    return ZonedDateTime.parse(attributeValues.get(TIMESTAMP_KEY).s());
  }

  private Object deserializePayload(Map<String, AttributeValue> attributeValues,
                                    EntityReference entityReference,
                                    String eventName,
                                    long sequenceNumber) {
    try {
      return eventPayloadDeserializer.deserializePayload(
          attributeValues.get(PAYLOAD_KEY).s(),
          new EventType(entityReference.entityName(), eventName));
    } catch (Exception exception) {
      throw new EventStoreException(
          "Could not payload for %s".formatted(
              new EventReference(entityReference, sequenceNumber, eventName)),
          exception);
    }
  }
}
