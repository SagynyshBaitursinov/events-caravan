package io.saga.caravan.event.sourcing.dynamodb;

import io.saga.caravan.entity.EntityReference;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class EntityReferenceKeyUtils {

  private static final String SEPARATOR = "#";

  static String toKeyValue(EntityReference entityReference) {
    requireFreeOfSeparator(entityReference);

    return entityReference.entityName() + SEPARATOR + entityReference.entityId();
  }

  static String toShardedKeyValue(EntityReference entityReference, long shardIndex) {
    return toKeyValue(entityReference) + SEPARATOR + shardIndex;
  }

  private static void requireFreeOfSeparator(EntityReference entityReference) {
    if (entityReference.entityName().contains(SEPARATOR)
        || entityReference.entityId().contains(SEPARATOR)) {
      throw new IllegalArgumentException(
          "EntityReference must not contain '%s' to be stored in DynamoDB, got %s"
              .formatted(SEPARATOR, entityReference));
    }
  }
}
