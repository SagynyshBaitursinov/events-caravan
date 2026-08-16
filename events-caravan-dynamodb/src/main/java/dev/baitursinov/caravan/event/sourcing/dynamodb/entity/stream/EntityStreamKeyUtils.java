package dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.sourcing.dynamodb.DynamoDbStoreException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EntityStreamKeyUtils {

  public static final String SEPARATOR = "#";

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      new DateTimeFormatterBuilder().appendInstant(3).toFormatter();

  static String toPartitionKeyValue(String entityName, String timeBucket, int shardIndex) {
    return entityName + SEPARATOR + timeBucket + SEPARATOR + shardIndex;
  }

  static String toSortKeyValue(ZonedDateTime firstEventTimestamp, String entityId) {
    return TIMESTAMP_FORMATTER.format(firstEventTimestamp) + SEPARATOR + entityId;
  }

  static void requireFreeOfSeparator(EntityReference entityReference) {
    if (entityReference.entityName().contains(SEPARATOR)
        || entityReference.entityId().contains(SEPARATOR)) {
      throw new DynamoDbStoreException(
          "EntityReference must not contain '%s' to be stored in the entity stream, got %s"
              .formatted(SEPARATOR, entityReference));
    }
  }
}
