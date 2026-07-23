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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@Component
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

  private final DynamoDbClient dynamoDbClient;
  private final String eventsTableName;
  private final Integer maxPageSize;
  private final long partitionShardSize;
  private final EventPayloadSerializer eventPayloadSerializer;
  private final EventPayloadDeserializer eventPayloadDeserializer;

  public DynamoDbBasedEventStore(DynamoDbClient dynamoDbClient,
                                 @Value("${caravan.event.store.dynamo-db.table-name}") String eventsTableName,
                                 @Value("${caravan.event.store.dynamo-db.query-max-page-size:#{null}}") Integer maxPageSize,
                                 @Value("${caravan.event.store.dynamo-db.partition-shard-size:10000}") long partitionShardSize,
                                 EventPayloadSerializer eventPayloadSerializer,
                                 EventPayloadDeserializer eventPayloadDeserializer) {
    this.dynamoDbClient = dynamoDbClient;
    this.eventsTableName = eventsTableName;
    this.maxPageSize = maxPageSize;
    if (partitionShardSize <= 0) {
      throw new IllegalArgumentException(
          "caravan.event.store.dynamo-db.partition-shard-size must be positive, got %d"
              .formatted(partitionShardSize));
    }
    this.partitionShardSize = partitionShardSize;
    this.eventPayloadSerializer = eventPayloadSerializer;
    this.eventPayloadDeserializer = eventPayloadDeserializer;
  }

  @Override
  public void produce(Event<?> event) {
    try {
      dynamoDbClient.putItem(
          PutItemRequest.builder()
              .tableName(eventsTableName)
              .item(mapEventToAttributes(event))
              .conditionExpression(NOT_YET_EXISTS_CONDITION_EXPRESSION)
              .expressionAttributeNames(NOT_YET_EXISTS_EXPRESSION_ATTRIBUTE_NAMES)
              .build());
    } catch (ConditionalCheckFailedException exception) {
      throw duplicateEventProductionException(event);
    }
  }

  @Override
  public void produce(List<Event<?>> events) {
    if (events.isEmpty()) {
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
    } catch (DynamoDbException exception) {
      throw new EventProductionException(
          "Failed to produce %d events in a transaction: %s"
              .formatted(events.size(), exception.getMessage()));
    }
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
            toShardedEntityReferenceValue(
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
            event.timestamp().toString()));
    attributes.put(
        PAYLOAD_KEY,
        AttributeValue.fromS(
            eventPayloadSerializer.serializePayload(event)));
    return attributes;
  }

  private String toEventReferenceStringValue(EntityReference entityReference) {
    return entityReference.entityName() + "#" + entityReference.entityId();
  }

  /**
   * Sequence numbers are gapless and start at 1 (enforced by EventSourcedEntity and the
   * all-or-nothing writes below), so every shard except an entity's current tip ends up holding
   * exactly {@code partitionShardSize} events. That lets reads detect a shard boundary purely
   * from the highest sequence number observed, without a separate counter item.
   */
  private long shardIndexForSequenceNumber(long sequenceNumber) {
    return (sequenceNumber - 1) / partitionShardSize;
  }

  private String toShardedEntityReferenceValue(EntityReference entityReference, long shardIndex) {
    return toEventReferenceStringValue(entityReference) + "#" + shardIndex;
  }

  @Override
  public Stream<Event<?>> getEventsOfEntity(EntityReference entityReference,
                                            long fromSequenceNumberExclusive) {
    if (fromSequenceNumberExclusive < 0) {
      throw new IllegalArgumentException(
          "fromSequenceNumberExclusive must not be negative (sequence numbers start at 1; "
              + "0 means from the beginning), got %d".formatted(fromSequenceNumberExclusive));
    }
    return StreamSupport.stream(
        createSpliterator(entityReference, fromSequenceNumberExclusive), false);
  }

  private DynamoDbEventsSpliterator createSpliterator(EntityReference entityReference,
                                                      long fromSequenceNumberExclusive) {
    long firstShardIndex = shardIndexForSequenceNumber(fromSequenceNumberExclusive + 1);
    boolean resumesExactlyAtShardBoundary = fromSequenceNumberExclusive % partitionShardSize == 0;

    Map<String, AttributeValue> firstShardExclusiveStartKey = resumesExactlyAtShardBoundary
        ? null
        : Map.of(
        PK,
        AttributeValue.fromS(
            toShardedEntityReferenceValue(entityReference, firstShardIndex)),
        SK,
        AttributeValue.fromN(
            String.valueOf(fromSequenceNumberExclusive)));

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
                    toShardedEntityReferenceValue(entityReference, shardIndex))))
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
