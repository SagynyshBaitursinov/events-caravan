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
 * Iterates over single entity's events across its sharded partitions in ascending sequence-number
 * order. Events for an entity are spread across partition keys suffixed by
 * {@code sequenceNumber/partitionShardSize}, so once a shard's own DynamoDB pagination is
 * exhausted, the highest sequence number observed in that shard is compared against the shard's
 * upper bound: an exact match means the shard was filled to capacity and a subsequent shard may
 * exist, anything less means this is the entity's last partition.
 */
public class DynamoDbEventsSpliterator implements Spliterator<Event<?>> {

  private final DynamoDbClient dynamoDbClient;
  private final long partitionShardSize;
  private final String sequenceNumberAttributeName;
  private final LongFunction<QueryRequest.Builder> shardQueryBuilderFactory;
  private final EventMapper eventMapper;

  private long currentShardIndex;
  @Nullable
  private final Map<String, AttributeValue> firstShardExclusiveStartKey;
  @Nullable
  private Map<String, AttributeValue> withinShardLastEvaluatedKey;
  private long lastSequenceNumberSeenInShard = -1;

  private Iterator<Map<String, AttributeValue>> currentPageIterator = Collections.emptyIterator();

  private boolean initialized = false;
  private boolean finished = false;

  public DynamoDbEventsSpliterator(DynamoDbClient dynamoDbClient,
                                   long partitionShardSize,
                                   String sequenceNumberAttributeName,
                                   long firstShardIndex,
                                   @Nullable Map<String, AttributeValue> firstShardExclusiveStartKey,
                                   LongFunction<QueryRequest.Builder> shardQueryBuilderFactory,
                                   EventMapper eventMapper) {
    this.dynamoDbClient = dynamoDbClient;
    this.partitionShardSize = partitionShardSize;
    this.sequenceNumberAttributeName = sequenceNumberAttributeName;
    this.currentShardIndex = firstShardIndex;
    this.firstShardExclusiveStartKey = firstShardExclusiveStartKey;
    this.shardQueryBuilderFactory = shardQueryBuilderFactory;
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
      lastSequenceNumberSeenInShard =
          Long.parseLong(items.getLast().get(sequenceNumberAttributeName).n());
    }

    withinShardLastEvaluatedKey = response.hasLastEvaluatedKey()
        ? response.lastEvaluatedKey()
        : null;
  }

  @Nullable
  private QueryRequest nextRequest() {
    if (!initialized) {
      return shardQueryBuilderFactory.apply(currentShardIndex)
          .exclusiveStartKey(firstShardExclusiveStartKey)
          .build();
    }

    if (withinShardLastEvaluatedKey != null) {
      return shardQueryBuilderFactory.apply(currentShardIndex)
          .exclusiveStartKey(withinShardLastEvaluatedKey)
          .build();
    }

    if (lastSequenceNumberSeenInShard != shardUpperBound(currentShardIndex)) {
      return null;
    }

    currentShardIndex++;
    lastSequenceNumberSeenInShard = -1;
    return shardQueryBuilderFactory.apply(currentShardIndex).build();
  }

  private long shardUpperBound(long shardIndex) {
    return (shardIndex + 1) * partitionShardSize;
  }
}
