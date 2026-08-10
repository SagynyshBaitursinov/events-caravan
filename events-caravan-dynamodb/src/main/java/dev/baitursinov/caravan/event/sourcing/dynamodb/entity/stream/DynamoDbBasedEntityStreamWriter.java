package dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream;

import dev.baitursinov.caravan.event.sourcing.dynamodb.DynamoDbSetupException;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamEntry;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamWriter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;

import static dev.baitursinov.caravan.event.sourcing.dynamodb.DynamoDbUtils.requireTableIsActive;
import static dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream.EntityStreamKeyUtils.requireFreeOfSeparator;
import static dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream.EntityStreamKeyUtils.shardIndexOf;
import static dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream.EntityStreamKeyUtils.toPartitionKeyValue;
import static dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream.EntityStreamKeyUtils.toSortKeyValue;
import static dev.baitursinov.caravan.utils.TextUtils.hasText;
import static java.util.Objects.requireNonNull;

/**
 * Writes entities into the DynamoDB entity stream table: a sharded, time-bucketed table of
 * every entity that has ever produced an event
 */
@Slf4j
public class DynamoDbBasedEntityStreamWriter implements EntityStreamWriter {

  private static final String PK = "PK";
  private static final String SK = "SK";

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;
  private final int shardCount;
  private final TimeBucket timeBucket;

  public DynamoDbBasedEntityStreamWriter(DynamoDbClient dynamoDbClient,
                                         String tableName,
                                         TimeBucket timeBucket,
                                         int shardCount) {
    requireNonNull(dynamoDbClient, "dynamoDbClient cannot be null");
    requireNonNull(timeBucket, "timeBucket cannot be null");

    if (shardCount <= 0) {
      throw new DynamoDbSetupException(
          "shardCount must be positive, got %d".formatted(shardCount));
    }

    if (!hasText(tableName)) {
      throw new DynamoDbSetupException("tableName must be set");
    }
    requireTableIsActive(dynamoDbClient, tableName);

    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
    this.shardCount = shardCount;
    this.timeBucket = timeBucket;
  }

  /**
   * Idempotent: the whole item is deterministic from {@code entry}, so a redelivered first
   * event re-writes the byte-identical item.
   */
  @Override
  public void write(EntityStreamEntry entry) {
    requireFreeOfSeparator(entry.entityReference());

    log.debug("Writing {} into the entity stream", entry.entityReference());
    dynamoDbClient.putItem(
        PutItemRequest.builder()
            .tableName(tableName)
            .item(mapEntryToAttributes(entry))
            .build());
    log.debug("Wrote {} into the entity stream", entry.entityReference());
  }

  private Map<String, AttributeValue> mapEntryToAttributes(EntityStreamEntry entry) {
    var entityReference = entry.entityReference();
    var timeBucket = this.timeBucket.bucketOf(entry.firstEventTimestamp());
    var shardIndex = shardIndexOf(entityReference.entityId(), shardCount);

    return Map.of(
        PK,
        AttributeValue.fromS(
            toPartitionKeyValue(entityReference.entityName(), timeBucket, shardIndex)),
        SK,
        AttributeValue.fromS(
            toSortKeyValue(entry.firstEventTimestamp(), entityReference.entityId())));
  }
}
