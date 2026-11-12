package dev.baitursinov.caravan.event.sourcing.dynamodb;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.sourcing.EventStoreException;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.LongFunction;

/**
 * Iterates over single entity's events across its sliced partitions in ascending sequence-number
 * order. Events for an entity are spread across partition keys suffixed by
 * {@code sequenceNumber/partitionSliceSize}, so after each page the highest sequence number
 * observed in the slice is compared against the slice's upper bound: an exact match means the
 * slice was filled to capacity and iteration jumps straight to the next slice (skipping any
 * remaining DynamoDB pagination, which would only return an empty page); otherwise the slice's
 * own pagination is followed until exhausted, and a slice ending below its upper bound is the
 * entity's last partition.
 */
public class DynamoDbEventsSpliterator implements Spliterator<Event<?>> {

  private final DynamoDbClient dynamoDbClient;
  private final long partitionSliceSize;
  private final String sequenceNumberAttributeName;
  private final LongFunction<QueryRequest.Builder> sliceQueryBuilderFactory;
  private final EventMapper eventMapper;

  private long currentSliceIndex;
  @Nullable
  private final Map<String, AttributeValue> firstSliceExclusiveStartKey;
  @Nullable
  private Map<String, AttributeValue> withinSliceLastEvaluatedKey;
  private long lastSequenceNumberSeenInSlice = -1;

  private Iterator<Map<String, AttributeValue>> currentPageIterator = Collections.emptyIterator();

  private boolean initialized = false;
  private boolean finished = false;

  public DynamoDbEventsSpliterator(DynamoDbClient dynamoDbClient,
                                   long partitionSliceSize,
                                   String sequenceNumberAttributeName,
                                   long firstSliceIndex,
                                   @Nullable Map<String, AttributeValue> firstSliceExclusiveStartKey,
                                   LongFunction<QueryRequest.Builder> sliceQueryBuilderFactory,
                                   EventMapper eventMapper) {
    this.dynamoDbClient = dynamoDbClient;
    this.partitionSliceSize = partitionSliceSize;
    this.sequenceNumberAttributeName = sequenceNumberAttributeName;
    this.currentSliceIndex = firstSliceIndex;
    this.firstSliceExclusiveStartKey = firstSliceExclusiveStartKey;
    this.sliceQueryBuilderFactory = sliceQueryBuilderFactory;
    this.eventMapper = eventMapper;
  }

  @Override
  public boolean tryAdvance(Consumer<? super Event<?>> action) {
    ensureInitialized();

    while (!currentPageIterator.hasNext()) {
      if (finished) {
        return false;
      }

      try {
        fetchNextPage();
      } catch (Exception exception) {
        throw new EventStoreException(exception);
      }
    }

    Event<?> event = eventMapper.mapAttributesToEvent(currentPageIterator.next());
    action.accept(event);
    return true;
  }

  @Nullable
  @Override
  public Spliterator<Event<?>> trySplit() {
    return null;
  }

  @Override
  public long estimateSize() {
    return Long.MAX_VALUE;
  }

  @Override
  public int characteristics() {
    return NONNULL | ORDERED | DISTINCT | IMMUTABLE;
  }

  private void ensureInitialized() {
    if (!initialized) {
      try {
        fetchNextPage();
      } catch (Exception exception) {
        throw new EventStoreException(exception);
      }
      initialized = true;
    }
  }

  private void fetchNextPage() {
    QueryRequest request = nextRequest();
    if (request == null) {
      finished = true;
      currentPageIterator = Collections.emptyIterator();
      return;
    }

    QueryResponse response = dynamoDbClient.query(request);

    List<Map<String, AttributeValue>> items = response.items();
    currentPageIterator = items.iterator();
    if (!items.isEmpty()) {
      lastSequenceNumberSeenInSlice =
          Long.parseLong(items.getLast().get(sequenceNumberAttributeName).n());
    }

    withinSliceLastEvaluatedKey = response.hasLastEvaluatedKey()
        ? response.lastEvaluatedKey()
        : null;
  }

  @Nullable
  private QueryRequest nextRequest() {
    if (!initialized) {
      return sliceQueryBuilderFactory.apply(currentSliceIndex)
          .exclusiveStartKey(firstSliceExclusiveStartKey)
          .build();
    }

    if (lastSequenceNumberSeenInSlice == sliceUpperBound(currentSliceIndex)) {
      proceedToNextSlice();
      return sliceQueryBuilderFactory.apply(currentSliceIndex).build();
    }

    if (withinSliceLastEvaluatedKey != null) {
      return sliceQueryBuilderFactory.apply(currentSliceIndex)
          .exclusiveStartKey(withinSliceLastEvaluatedKey)
          .build();
    }

    return null;
  }

  private void proceedToNextSlice() {
    currentSliceIndex++;
    lastSequenceNumberSeenInSlice = -1;
    withinSliceLastEvaluatedKey = null;
  }

  private long sliceUpperBound(long sliceIndex) {
    return (sliceIndex + 1) * partitionSliceSize;
  }
}
