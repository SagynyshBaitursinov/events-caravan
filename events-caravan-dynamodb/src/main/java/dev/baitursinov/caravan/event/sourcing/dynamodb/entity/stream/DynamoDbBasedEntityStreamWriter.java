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
import static dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream.EntityStreamKeyUtils.toPartitionKeyValue;
import static dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream.EntityStreamKeyUtils.toSortKeyValue;
import static dev.baitursinov.caravan.utils.TextUtils.hasText;
import static java.util.Objects.requireNonNull;

/**
 * Writes entities into the DynamoDB entity stream table: a sharded, time-bucketed table of
 * every entity that has ever produced an event. The time bucket and shard an entity is written
 * into are derived by the caller (see {@code EntityStreamWritingEventHandler}) from its
 * {@code EntityStreamRegistration} and passed in on every write.
 */
@Slf4j
public class DynamoDbBasedEntityStreamWriter implements EntityStreamWriter {

  private static final String PK = "PK";
  private static final String SK = "SK";

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;

  public DynamoDbBasedEntityStreamWriter(DynamoDbClient dynamoDbClient,
                                         String tableName) {
    requireNonNull(dynamoDbClient, "dynamoDbClient cannot be null");

    if (!hasText(tableName)) {
      throw new DynamoDbSetupException("tableName must be set");
    }
    requireTableIsActive(dynamoDbClient, tableName);

    this.dynamoDbClient = dynamoDbClient;
    this.tableName = tableName;
  }

  /**
   * Idempotent: the whole item is deterministic from {@code entry}, {@code timeBucketLocation}
   * and {@code shardLocation}, so a redelivered first event re-writes the byte-identical item.
   */
  @Override
  public void write(EntityStreamEntry entry,
                    String timeBucketLocation,
                    int shardLocation) {
    requireFreeOfSeparator(entry.entityReference());

    log.debug("Writing {} into the entity stream", entry.entityReference());
    dynamoDbClient.putItem(
        PutItemRequest.builder()
            .tableName(tableName)
            .item(mapEntryToAttributes(entry, timeBucketLocation, shardLocation))
            .build());
    log.debug("Wrote {} into the entity stream", entry.entityReference());
  }

  private Map<String, AttributeValue> mapEntryToAttributes(
      EntityStreamEntry entry, String timeBucketLocation, int shardLocation) {

    var entityReference = entry.entityReference();

    return Map.of(
        PK,
        AttributeValue.fromS(
            toPartitionKeyValue(entityReference.entityName(), timeBucketLocation, shardLocation)),
        SK,
        AttributeValue.fromS(
            toSortKeyValue(entry.firstEventTimestamp(), entityReference.entityId())));
  }
}
