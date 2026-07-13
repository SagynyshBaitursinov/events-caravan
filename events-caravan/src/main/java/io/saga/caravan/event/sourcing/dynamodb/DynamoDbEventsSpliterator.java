package io.saga.caravan.event.sourcing.dynamodb;

import io.saga.caravan.event.Event;
import org.jspecify.annotations.Nullable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

public class DynamoDbEventsSpliterator implements Spliterator<Event<?>> {

  private final DynamoDbClient dynamoDbClient;
  private final QueryRequest firstPageRequest;
  private final QueryRequest.Builder nextPageBuilder;
  private final EventMapper eventMapper;

  @Nullable
  private Iterator<Map<String, AttributeValue>> currentPageIterator;

  @Nullable
  private Map<String, AttributeValue> lastEvaluatedKey;

  private boolean initialized = false;

  public DynamoDbEventsSpliterator(DynamoDbClient dynamoDbClient,
                                   QueryRequest.Builder baseQueryBuilder,
                                   EventMapper eventMapper) {
    this.dynamoDbClient = dynamoDbClient;
    this.firstPageRequest = baseQueryBuilder.build();
    this.nextPageBuilder = baseQueryBuilder;
    this.eventMapper = eventMapper;
  }

  @Override
  public boolean tryAdvance(Consumer<? super Event<?>> action) {
    ensureInitialized();

    if (currentPageIterator == null) {
      throw new IllegalStateException("Current page iterator has not been initialized");
    }

    while (!currentPageIterator.hasNext()) {
      if (lastEvaluatedKey == null) {
        return false;
      }
      fetchNextPage();
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
      fetchNextPage();
      initialized = true;
    }
  }

  private void fetchNextPage() {
    QueryRequest request = lastEvaluatedKey == null
        ? firstPageRequest
        : nextPageBuilder.exclusiveStartKey(lastEvaluatedKey).build();

    QueryResponse response = dynamoDbClient.query(request);

    List<Map<String, AttributeValue>> items = response.items();
    currentPageIterator = items.iterator();

    lastEvaluatedKey = response.hasLastEvaluatedKey()
        ? response.lastEvaluatedKey()
        : null;
  }
}