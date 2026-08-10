package dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.sourcing.dynamodb.DynamoDbStoreException;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

import static dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream.EntityStreamKeyUtils.requireFreeOfSeparator;
import static dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream.EntityStreamKeyUtils.shardIndexOf;
import static dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream.EntityStreamKeyUtils.toPartitionKeyValue;
import static dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream.EntityStreamKeyUtils.toSortKeyValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityStreamKeyUtilsTest {

  @Test
  void shardIndexIsWithinBoundsAndDeterministic() {
    int shardIndex = shardIndexOf("entity-1", 16);

    assertThat(shardIndex).isBetween(0, 15);
    assertThat(shardIndexOf("entity-1", 16)).isEqualTo(shardIndex);
  }

  @Test
  void partitionKeyValueJoinsNameBucketAndShard() {
    assertThat(toPartitionKeyValue("Entity", "2026-08", 3)).isEqualTo("Entity#2026-08#3");
  }

  @Test
  void sortKeyValueIsFixedWidthTimestampFollowedByEntityId() {
    assertThat(toSortKeyValue(ZonedDateTime.parse("2026-08-10T14:03:22Z"), "entity-1"))
        .isEqualTo("2026-08-10T14:03:22.000Z#entity-1");
  }

  @Test
  void sortKeyValueOrdersLexicographicallyByInstant() {
    var earlier = toSortKeyValue(ZonedDateTime.parse("2026-08-10T00:00:00Z"), "entity-1");
    var later = toSortKeyValue(ZonedDateTime.parse("2026-08-10T00:00:00.123Z"), "entity-2");

    assertThat(earlier.compareTo(later)).isLessThan(0);
  }

  @Test
  void rejectsEntityReferenceContainingSeparator() {
    assertThatThrownBy(() -> requireFreeOfSeparator(new EntityReference("Entity#Name", "1")))
        .isExactlyInstanceOf(DynamoDbStoreException.class);
    assertThatThrownBy(() -> requireFreeOfSeparator(new EntityReference("Entity", "1#2")))
        .isExactlyInstanceOf(DynamoDbStoreException.class);
  }

  @Test
  void acceptsEntityReferenceFreeOfSeparator() {
    assertThatNoException()
        .isThrownBy(() -> requireFreeOfSeparator(new EntityReference("Entity", "1")));
  }
}
