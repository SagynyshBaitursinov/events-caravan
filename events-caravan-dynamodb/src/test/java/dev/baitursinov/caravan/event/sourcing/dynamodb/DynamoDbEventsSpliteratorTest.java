package dev.baitursinov.caravan.event.sourcing.dynamodb;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongFunction;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DynamoDbEventsSpliteratorTest {

  private static final String PK = "entityReference";
  private static final String SK = "sequenceNumber";
  private static final EntityReference ENTITY_REFERENCE = new EntityReference("Entity", "1");

  @Test
  void fullSliceRollsOverIntoTheNextSlice() {
    long partitionSliceSize = 3;
    DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);

    when(dynamoDbClient.query(any(QueryRequest.class))).thenAnswer(invocation -> {
      QueryRequest request = invocation.getArgument(0);
      String pkValue = request.expressionAttributeValues().get(":pkVal").s();

      if (pkValue.endsWith("#0")) {
        return QueryResponse.builder()
            .items(
                List.of(
                    item(1),
                    item(2),
                    item(3)))
            .build();
      }
      if (pkValue.endsWith("#1")) {
        return QueryResponse.builder()
            .items(
                List.of(
                    item(4),
                    item(5)))
            .build();
      }
      throw new AssertionError("Unexpected slice queried: " + pkValue);
    });

    var spliterator = freshSpliterator(dynamoDbClient, partitionSliceSize, 0);

    List<Long> sequenceNumbers = StreamSupport.stream(spliterator, false)
        .map(Event::sequenceNumber)
        .toList();

    assertThat(sequenceNumbers).containsExactly(1L, 2L, 3L, 4L, 5L);
  }

  @Test
  void sliceHoldingFewerThanPartitionSliceSizeEventsStopsWithoutQueryingTheNextSlice() {
    long partitionSliceSize = 3;
    DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);

    when(dynamoDbClient.query(any(QueryRequest.class))).thenAnswer(invocation -> {
      QueryRequest request = invocation.getArgument(0);
      String pkValue = request.expressionAttributeValues().get(":pkVal").s();

      if (pkValue.endsWith("#0")) {
        return QueryResponse.builder()
            .items(
                List.of(
                    item(1),
                    item(2)))
            .build();
      }
      throw new AssertionError("Slice 1 should never be queried: " + pkValue);
    });

    var spliterator = freshSpliterator(dynamoDbClient, partitionSliceSize, 0);

    List<Long> sequenceNumbers = StreamSupport.stream(spliterator, false)
        .map(Event::sequenceNumber)
        .toList();

    assertThat(sequenceNumbers).containsExactly(1L, 2L);
  }

  @Test
  void consecutiveFullSlicesKeepRollingOverUntilPartialSliceIsFound() {
    long partitionSliceSize = 2;
    DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);

    when(dynamoDbClient.query(any(QueryRequest.class))).thenAnswer(invocation -> {
      QueryRequest request = invocation.getArgument(0);
      String pkValue = request.expressionAttributeValues().get(":pkVal").s();

      if (pkValue.endsWith("#0")) {
        return QueryResponse.builder().items(
            List.of(item(1), item(2))).build();
      }
      if (pkValue.endsWith("#1")) {
        return QueryResponse.builder().items(
            List.of(item(3), item(4))).build();
      }
      if (pkValue.endsWith("#2")) {
        return QueryResponse.builder().items(
            List.of(item(5))).build();
      }
      throw new AssertionError("Unexpected slice queried: " + pkValue);
    });

    var spliterator = freshSpliterator(dynamoDbClient, partitionSliceSize, 0);

    List<Long> sequenceNumbers = StreamSupport.stream(spliterator, false)
        .map(Event::sequenceNumber)
        .toList();

    assertThat(sequenceNumbers).containsExactly(1L, 2L, 3L, 4L, 5L);
  }

  @Test
  void emptySliceWithNoResumePointYieldsNoEventsAndQueriesOnlyOnce() {
    long partitionSliceSize = 3;
    DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);

    when(dynamoDbClient.query(any(QueryRequest.class)))
        .thenReturn(QueryResponse.builder().items(List.of()).build());

    var spliterator = freshSpliterator(dynamoDbClient, partitionSliceSize, 0);

    List<Long> sequenceNumbers = StreamSupport.stream(spliterator, false)
        .map(Event::sequenceNumber)
        .toList();

    assertThat(sequenceNumbers).isEmpty();
    verify(dynamoDbClient, times(1)).query(any(QueryRequest.class));
  }

  @Test
  void sliceSplitAcrossSeveralDynamoDbPagesIsFullyDrainedUsingEachPagesOwnLastEvaluatedKey() {
    long partitionSliceSize = 5;
    DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);
    Map<String, AttributeValue> lastEvaluatedKeyOfFirstPage = Map.of(SK, AttributeValue.fromN("2"));

    when(dynamoDbClient.query(any(QueryRequest.class)))
        .thenReturn(
            QueryResponse.builder()
                .items(List.of(item(1), item(2)))
                .lastEvaluatedKey(lastEvaluatedKeyOfFirstPage)
                .build())
        .thenReturn(
            QueryResponse.builder()
                .items(List.of(item(3)))
                .build());

    var spliterator = freshSpliterator(dynamoDbClient, partitionSliceSize, 0);

    List<Long> sequenceNumbers = StreamSupport.stream(spliterator, false)
        .map(Event::sequenceNumber)
        .toList();

    assertThat(sequenceNumbers).containsExactly(1L, 2L, 3L);

    ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
    verify(dynamoDbClient, times(2)).query(captor.capture());

    QueryRequest secondPageRequest = captor.getAllValues().get(1);
    assertThat(secondPageRequest.exclusiveStartKey()).isEqualTo(lastEvaluatedKeyOfFirstPage);
  }

  @Test
  void pageEndingExactlyAtSliceUpperBoundJumpsToNextSliceWithoutFollowingLastEvaluatedKey() {
    long partitionSliceSize = 2;
    DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);

    when(dynamoDbClient.query(any(QueryRequest.class))).thenAnswer(invocation -> {
      QueryRequest request = invocation.getArgument(0);
      String pkValue = request.expressionAttributeValues().get(":pkVal").s();

      if (pkValue.endsWith("#0")) {
        if (request.hasExclusiveStartKey()) {
          return QueryResponse.builder().items(List.of()).build();
        }
        return QueryResponse.builder()
            .items(List.of(item(1), item(2)))
            .lastEvaluatedKey(Map.of(SK, AttributeValue.fromN("2")))
            .build();
      }
      if (pkValue.endsWith("#1")) {
        return QueryResponse.builder().items(
            List.of(item(3))).build();
      }
      throw new AssertionError("Unexpected slice queried: " + pkValue);
    });

    var spliterator = freshSpliterator(dynamoDbClient, partitionSliceSize, 0);

    List<Long> sequenceNumbers = StreamSupport.stream(spliterator, false)
        .map(Event::sequenceNumber)
        .toList();

    assertThat(sequenceNumbers).containsExactly(1L, 2L, 3L);

    var queryRequestsCaptor = ArgumentCaptor.forClass(QueryRequest.class);
    verify(dynamoDbClient, times(2)).query(queryRequestsCaptor.capture());

    var secondRequest = queryRequestsCaptor.getAllValues().get(1);
    assertThat(secondRequest.expressionAttributeValues().get(":pkVal").s()).endsWith("#1");
    assertThat(secondRequest.hasExclusiveStartKey()).isFalse();
  }

  @Test
  void suppliedFirstSliceExclusiveStartKeyIsSentOnTheFirstRequestOnly() {
    long partitionSliceSize = 5;
    DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);
    Map<String, AttributeValue> resumeKey = Map.of(
        PK, AttributeValue.fromS("Entity#1#0"),
        SK, AttributeValue.fromN("1"));

    when(dynamoDbClient.query(any(QueryRequest.class))).thenAnswer(invocation -> {
      QueryRequest request = invocation.getArgument(0);
      String pkValue = request.expressionAttributeValues().get(":pkVal").s();

      if (pkValue.endsWith("#0")) {
        return QueryResponse.builder().items(
            List.of(item(2), item(3))).build();
      }
      throw new AssertionError("Slice 1 should never be queried: " + pkValue);
    });

    var spliterator = spliteratorResumingAt(dynamoDbClient, partitionSliceSize, 0, resumeKey);

    List<Long> sequenceNumbers = StreamSupport.stream(spliterator, false)
        .map(Event::sequenceNumber)
        .toList();

    assertThat(sequenceNumbers).containsExactly(2L, 3L);

    ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
    verify(dynamoDbClient).query(captor.capture());
    assertThat(captor.getValue().exclusiveStartKey()).isEqualTo(resumeKey);
  }

  @SuppressWarnings("SameParameterValue")
  private static DynamoDbEventsSpliterator freshSpliterator(DynamoDbClient dynamoDbClient,
                                                            long partitionSliceSize,
                                                            long firstSliceIndex) {
    return spliteratorResumingAt(dynamoDbClient, partitionSliceSize, firstSliceIndex, null);
  }

  @SuppressWarnings("SameParameterValue")
  private static DynamoDbEventsSpliterator spliteratorResumingAt(
      DynamoDbClient dynamoDbClient,
      long partitionSliceSize,
      long firstSliceIndex,
      @Nullable Map<String, AttributeValue> firstSliceExclusiveStartKey) {
    LongFunction<QueryRequest.Builder> sliceQueryBuilderFactory = sliceIndex ->
        QueryRequest.builder()
            .tableName("events")
            .keyConditionExpression("#pk = :pkVal")
            .expressionAttributeNames(Map.of("#pk", PK))
            .expressionAttributeValues(
                Map.of(":pkVal", AttributeValue.fromS("Entity#1#" + sliceIndex)));

    return new DynamoDbEventsSpliterator(
        dynamoDbClient,
        partitionSliceSize,
        SK,
        firstSliceIndex,
        firstSliceExclusiveStartKey,
        sliceQueryBuilderFactory,
        DynamoDbEventsSpliteratorTest::mapToEvent);
  }

  private static Map<String, AttributeValue> item(long sequenceNumber) {
    var attributes = new LinkedHashMap<String, AttributeValue>();
    attributes.put(SK, AttributeValue.fromN(String.valueOf(sequenceNumber)));
    return attributes;
  }

  private static Event<?> mapToEvent(Map<String, AttributeValue> attributes) {
    return Event.builder()
        .entityReference(ENTITY_REFERENCE)
        .eventName("Incremented")
        .sequenceNumber(Long.parseLong(attributes.get(SK).n()))
        .timestamp(ZonedDateTime.now())
        .payload(new Object())
        .build();
  }
}
