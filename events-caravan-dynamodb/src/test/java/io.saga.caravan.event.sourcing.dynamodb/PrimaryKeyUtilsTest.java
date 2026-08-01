package io.saga.caravan.event.sourcing.dynamodb;

import io.saga.caravan.entity.EntityReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PrimaryKeyUtilsTest {

  @Test
  void flattensEntityReference() {
    assertThat(PrimaryKeyUtils.toPartitionKeyValue(new EntityReference("calculator", "1")))
        .isEqualTo("calculator#1");
  }

  @Test
  void appendsShardIndex() {
    assertThat(PrimaryKeyUtils.toShardedPartitionKeyValue(new EntityReference("calculator", "1"), 2))
        .isEqualTo("calculator#1#2");
  }

  @Test
  void rejectsSeparatorInEntityName() {
    assertThatThrownBy(() ->
        PrimaryKeyUtils.toPartitionKeyValue(new EntityReference("calculator#1", "2")))
        .isExactlyInstanceOf(DynamoDbStoreException.class)
        .hasMessageContaining("calculator#1");
  }

  @Test
  void rejectsSeparatorInEntityId() {
    assertThatThrownBy(() ->
        PrimaryKeyUtils.toPartitionKeyValue(new EntityReference("calculator", "1#2")))
        .isExactlyInstanceOf(DynamoDbStoreException.class)
        .hasMessageContaining("1#2");
  }

  @Test
  void distinctEntitiesCannotBeFlattenedIntoOneKey() {
    assertThatThrownBy(() ->
        PrimaryKeyUtils.toPartitionKeyValue(new EntityReference("a#b", "c")))
        .isExactlyInstanceOf(DynamoDbStoreException.class);
    assertThatThrownBy(() ->
        PrimaryKeyUtils.toPartitionKeyValue(new EntityReference("a", "b#c")))
        .isExactlyInstanceOf(DynamoDbStoreException.class);
  }

  @Test
  void rejectsSeparatorWhenSharding() {
    assertThatThrownBy(() ->
        PrimaryKeyUtils.toShardedPartitionKeyValue(new EntityReference("a", "b#c"), 0))
        .isExactlyInstanceOf(DynamoDbStoreException.class);
  }
}
