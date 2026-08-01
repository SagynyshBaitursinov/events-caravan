package io.saga.caravan.event.sourcing.dynamodb;

import io.saga.caravan.entity.EntityReference;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PrimaryKeyUtils {

  public static final String SEPARATOR = "#";

  static String toPartitionKeyValue(EntityReference entityReference) {
    requireFreeOfSeparator(entityReference);

    return entityReference.entityName() + SEPARATOR + entityReference.entityId();
  }

  static String toShardedPartitionKeyValue(EntityReference entityReference, long shardIndex) {
    return toPartitionKeyValue(entityReference) + SEPARATOR + shardIndex;
  }

  private static void requireFreeOfSeparator(EntityReference entityReference) {
    if (entityReference.entityName().contains(SEPARATOR)
        || entityReference.entityId().contains(SEPARATOR)) {
      throw new DynamoDbStoreException(
          "EntityReference must not contain '%s' to be stored in DynamoDB, got %s"
              .formatted(SEPARATOR, entityReference));
    }
  }
}
