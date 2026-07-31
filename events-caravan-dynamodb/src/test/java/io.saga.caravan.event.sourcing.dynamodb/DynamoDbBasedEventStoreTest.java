package io.saga.caravan.event.sourcing.dynamodb;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.Event;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.producer.DuplicateEventProductionException;
import io.saga.caravan.event.producer.EventProductionException;
import io.saga.caravan.event.serialization.EventPayloadDeserializer;
import io.saga.caravan.event.serialization.EventPayloadSerializer;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class DynamoDbBasedEventStoreTest {

  private static final EntityReference ENTITY_REFERENCE = new EntityReference("Entity", "reference-1");
  private static final long PARTITION_SHARD_SIZE = 100;

  DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);
  EventPayloadDeserializer eventPayloadDeserializer = mock(EventPayloadDeserializer.class);
  EventPayloadSerializer eventPayloadSerializer = mock(EventPayloadSerializer.class);

  DynamoDbBasedEventStore eventStore = new DynamoDbBasedEventStore(
      dynamoDbClient,
      eventPayloadSerializer,
      eventPayloadDeserializer,
      "events-table",
      100,
      PARTITION_SHARD_SIZE);

  @Test
  @SneakyThrows
  void resumingExactlyAtShardBoundaryQueriesOnlyTheNextShardWithNoExclusiveStartKey() {
    when(eventPayloadDeserializer.deserializePayload(any(), any(EventType.class))).thenReturn(new Object());
    when(dynamoDbClient.query(any(QueryRequest.class)))
        .thenReturn(
            QueryResponse.builder()
                .items(
                    List.of(item(101), item(102)))
                .build());

    List<Long> sequenceNumbers = eventStore.getEventsOfEntity(ENTITY_REFERENCE, 100)
        .map(Event::sequenceNumber)
        .toList();

    assertThat(sequenceNumbers).containsExactly(101L, 102L);

    ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
    verify(dynamoDbClient).query(captor.capture());

    QueryRequest issuedQuery = captor.getValue();
    assertThat(issuedQuery.expressionAttributeValues().get(":pkVal").s())
        .isEqualTo("Entity#reference-1#1");
    assertThat(issuedQuery.hasExclusiveStartKey()).isFalse();
  }

  @Test
  void startingFromZero() {
    when(dynamoDbClient.query(any(QueryRequest.class)))
        .thenReturn(QueryResponse.builder().items(List.of()).build());

    var list = eventStore.getEventsOfEntity(ENTITY_REFERENCE, 0).toList();
    assertThat(list).isEmpty();

    ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
    verify(dynamoDbClient).query(captor.capture());

    QueryRequest issuedQuery = captor.getValue();
    assertThat(issuedQuery.expressionAttributeValues().get(":pkVal").s())
        .isEqualTo("Entity#reference-1#0");
    assertThat(issuedQuery.hasExclusiveStartKey()).isFalse();
  }

  @Test
  void resumingMidShardQueriesTheContainingShardWithExclusiveStartKey() {
    when(dynamoDbClient.query(any(QueryRequest.class)))
        .thenReturn(QueryResponse.builder().items(List.of()).build());

    var list = eventStore.getEventsOfEntity(ENTITY_REFERENCE, 50).toList();
    assertThat(list).isEmpty();

    ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
    verify(dynamoDbClient).query(captor.capture());

    QueryRequest issuedQuery = captor.getValue();
    assertThat(issuedQuery.expressionAttributeValues().get(":pkVal").s())
        .isEqualTo("Entity#reference-1#0");
    assertThat(issuedQuery.exclusiveStartKey().get("sequenceNumber").n()).isEqualTo("50");
  }

  @Test
  void resumingMidShardQueriesTheContainingShardWithExclusiveStartKeyOnShardIndexOne() {
    when(dynamoDbClient.query(any(QueryRequest.class)))
        .thenReturn(QueryResponse.builder().items(List.of()).build());

    var list = eventStore.getEventsOfEntity(ENTITY_REFERENCE, 150).toList();
    assertThat(list).isEmpty();

    ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
    verify(dynamoDbClient).query(captor.capture());

    QueryRequest issuedQuery = captor.getValue();
    assertThat(issuedQuery.expressionAttributeValues().get(":pkVal").s())
        .isEqualTo("Entity#reference-1#1");
    assertThat(issuedQuery.exclusiveStartKey().get("sequenceNumber").n()).isEqualTo("150");
  }

  @Test
  void cannotEnterNegativeAsSequenceNumber() {
    assertThatThrownBy(() -> eventStore.getEventsOfEntity(ENTITY_REFERENCE, -1))
        .isExactlyInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void producingSingleEventPutsItemKeyedByItsShard() {
    when(eventPayloadSerializer.serializePayload(any())).thenReturn("{\"n\":1}");

    eventStore.produce(event(1));

    ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
    verify(dynamoDbClient).putItem(captor.capture());

    PutItemRequest issuedPut = captor.getValue();
    assertThat(issuedPut.tableName()).isEqualTo("events-table");
    assertThat(issuedPut.item().get("entityReference").s()).isEqualTo("Entity#reference-1#0");
    assertThat(issuedPut.item().get("sequenceNumber").n()).isEqualTo("1");
    assertThat(issuedPut.item().get("eventName").s()).isEqualTo("Incremented");
    assertThat(issuedPut.item().get("payload").s()).isEqualTo("{\"n\":1}");
    assertThat(issuedPut.conditionExpression())
        .isEqualTo("attribute_not_exists(#pk) AND attribute_not_exists(#sk)");
    assertThat(issuedPut.expressionAttributeNames()).containsEntry("#pk", "entityReference").containsEntry("#sk", "sequenceNumber");
  }

  @Test
  void producingEventPastTheShardBoundaryUsesTheNextShardsPartitionKey() {
    when(eventPayloadSerializer.serializePayload(any())).thenReturn("{}");

    eventStore.produce(event(PARTITION_SHARD_SIZE + 1));

    ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
    verify(dynamoDbClient).putItem(captor.capture());

    assertThat(captor.getValue().item().get("entityReference").s()).isEqualTo("Entity#reference-1#1");
    assertThat(captor.getValue().item().get("sequenceNumber").n()).isEqualTo(String.valueOf(PARTITION_SHARD_SIZE + 1));
  }

  @Test
  void producingEventOnAWholeSecondStillWritesThreeFractionalDigits() {
    when(eventPayloadSerializer.serializePayload(any())).thenReturn("{}");

    eventStore.produce(event(1, ZonedDateTime.parse("2026-01-01T00:00:00Z")));

    ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
    verify(dynamoDbClient).putItem(captor.capture());

    assertThat(captor.getValue().item().get("timestamp").s()).isEqualTo("2026-01-01T00:00:00.000Z");
  }

  @Test
  void producingEventOutsideUtcWritesTheSameInstantAsUtc() {
    when(eventPayloadSerializer.serializePayload(any())).thenReturn("{}");

    eventStore.produce(event(1, ZonedDateTime.parse("2026-01-01T03:00:00.5+03:00")));

    ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
    verify(dynamoDbClient).putItem(captor.capture());

    assertThat(captor.getValue().item().get("timestamp").s()).isEqualTo("2026-01-01T00:00:00.500Z");
  }

  @Test
  void storedTimestampsOfTheSameSecondCompareLexicographicallyInInstantOrder() {
    when(eventPayloadSerializer.serializePayload(any())).thenReturn("{}");

    eventStore.produce(event(1, ZonedDateTime.parse("2026-01-01T00:00:00Z")));
    eventStore.produce(event(2, ZonedDateTime.parse("2026-01-01T00:00:00.123Z")));

    ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
    verify(dynamoDbClient, times(2)).putItem(captor.capture());

    List<String> storedTimestamps = captor.getAllValues().stream()
        .map(put -> put.item().get("timestamp").s())
        .toList();

    assertThat(storedTimestamps).isSorted();
  }

  @Test
  void producingDuplicateSingleEventThrowsDuplicateEventProductionException() {
    when(eventPayloadSerializer.serializePayload(any())).thenReturn("{}");
    when(dynamoDbClient.putItem(any(PutItemRequest.class)))
        .thenThrow(ConditionalCheckFailedException.builder().message("exists").build());

    assertThatThrownBy(() -> eventStore.produce(event(1)))
        .isExactlyInstanceOf(DuplicateEventProductionException.class)
        .hasMessageContaining("Entity")
        .hasMessageContaining("sequenceNumber=1");
  }

  @Test
  void producingEmptyListOfEventsDoesNothing() {
    eventStore.produce(List.of());

    verifyNoInteractions(dynamoDbClient);
  }

  @Test
  void producingSingleElementListDelegatesToSinglePutItem() {
    when(eventPayloadSerializer.serializePayload(any())).thenReturn("{}");

    eventStore.produce(List.of(event(1)));

    verify(dynamoDbClient).putItem(any(PutItemRequest.class));
    verify(dynamoDbClient, never()).transactWriteItems(any(TransactWriteItemsRequest.class));
  }

  @Test
  void producingSeveralEventsUsesTransactionWithOnePutPerEventKeyedByItsOwnShard() {
    when(eventPayloadSerializer.serializePayload(any())).thenReturn("{}");

    eventStore.produce(List.of(event(PARTITION_SHARD_SIZE), event(PARTITION_SHARD_SIZE + 1)));

    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(dynamoDbClient).transactWriteItems(captor.capture());
    verify(dynamoDbClient, never()).putItem(any(PutItemRequest.class));

    List<Map<String, AttributeValue>> items = captor.getValue().transactItems().stream()
        .map(transactWriteItem -> transactWriteItem.put().item())
        .toList();

    assertThat(items).hasSize(2);
    assertThat(items.get(0).get("entityReference").s()).isEqualTo("Entity#reference-1#0");
    assertThat(items.get(0).get("sequenceNumber").n()).isEqualTo(String.valueOf(PARTITION_SHARD_SIZE));
    assertThat(items.get(1).get("entityReference").s()).isEqualTo("Entity#reference-1#1");
    assertThat(items.get(1).get("sequenceNumber").n()).isEqualTo(String.valueOf(PARTITION_SHARD_SIZE + 1));
  }

  @SuppressWarnings("unchecked")
  @Test
  void producingMoreThanMaxTransactionItemsFailsWithoutCallingDynamoDb() {
    List<? extends Event<?>> events = LongStream.rangeClosed(1, 101)
        .mapToObj(DynamoDbBasedEventStoreTest::event)
        .toList();

    assertThatThrownBy(() -> eventStore.produce((List<Event<?>>) events))
        .isExactlyInstanceOf(EventProductionException.class);

    verifyNoInteractions(dynamoDbClient);
  }

  @Test
  void singleDuplicateInTransactionThrowsDuplicateEventProductionException() {
    when(eventPayloadSerializer.serializePayload(any())).thenReturn("{}");
    when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(TransactionCanceledException.builder()
            .message("cancelled")
            .cancellationReasons(
                CancellationReason.builder().code("None").build(),
                CancellationReason.builder().code("ConditionalCheckFailed").build())
            .build());

    assertThatThrownBy(() -> eventStore.produce(List.of(event(1), event(2))))
        .isExactlyInstanceOf(DuplicateEventProductionException.class)
        .hasMessageContaining("sequenceNumber=2");
  }

  @Test
  void multipleDuplicatesInTransactionAreAllReportedInTheFailureMessage() {
    when(eventPayloadSerializer.serializePayload(any())).thenReturn("{}");
    when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(TransactionCanceledException.builder()
            .message("cancelled")
            .cancellationReasons(
                CancellationReason.builder().code("ConditionalCheckFailed").build(),
                CancellationReason.builder().code("ConditionalCheckFailed").build())
            .build());

    assertThatThrownBy(() -> eventStore.produce(List.of(event(1), event(2))))
        .isExactlyInstanceOf(DuplicateEventProductionException.class)
        .hasMessageContaining("sequenceNumber=1")
        .hasMessageContaining("sequenceNumber=2");
  }

  @Test
  void transactionCancellationWithNoDuplicatesThrowsPlainEventProductionException() {
    when(eventPayloadSerializer.serializePayload(any())).thenReturn("{}");
    when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(TransactionCanceledException.builder()
            .message("cancelled due to throttling")
            .cancellationReasons(
                CancellationReason.builder().code("ThrottlingError").build(),
                CancellationReason.builder().code("None").build())
            .build());

    assertThatThrownBy(() -> eventStore.produce(List.of(event(1), event(2))))
        .isExactlyInstanceOf(EventProductionException.class)
        .isNotInstanceOf(DuplicateEventProductionException.class);
  }

  @Test
  void genericDynamoDbFailureDuringTransactionIsWrappedAsEventProductionException() {
    when(eventPayloadSerializer.serializePayload(any())).thenReturn("{}");
    when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
        .thenThrow(DynamoDbException.builder().message("service unavailable").build());

    assertThatThrownBy(() -> eventStore.produce(List.of(event(1), event(2))))
        .isExactlyInstanceOf(EventProductionException.class)
        .isNotInstanceOf(DuplicateEventProductionException.class)
        .hasMessageContaining("service unavailable");
  }

  private static Event<?> event(long sequenceNumber) {
    return event(sequenceNumber, ZonedDateTime.now());
  }

  private static Event<?> event(long sequenceNumber, ZonedDateTime timestamp) {
    return Event.builder()
        .entityReference(ENTITY_REFERENCE)
        .eventName("Incremented")
        .sequenceNumber(sequenceNumber)
        .timestamp(timestamp)
        .payload(new Object())
        .build();
  }

  private static Map<String, AttributeValue> item(long sequenceNumber) {
    var attributes = new LinkedHashMap<String, AttributeValue>();
    attributes.put("sequenceNumber", AttributeValue.fromN(String.valueOf(sequenceNumber)));
    attributes.put("eventName", AttributeValue.fromS("Incremented"));
    attributes.put("timestamp", AttributeValue.fromS("2026-01-01T00:00:00Z"));
    attributes.put("payload", AttributeValue.fromS("{}"));
    return attributes;
  }
}
