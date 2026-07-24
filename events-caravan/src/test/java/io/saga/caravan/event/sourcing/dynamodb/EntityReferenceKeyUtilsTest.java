package io.saga.caravan.event.sourcing.dynamodb;

import io.saga.caravan.entity.EntityReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityReferenceKeyUtilsTest {

  @Test
  void flattensEntityReference() {
    assertThat(EntityReferenceKeyUtils.toKeyValue(new EntityReference("calculator", "1")))
        .isEqualTo("calculator#1");
  }

  @Test
  void appendsShardIndex() {
    assertThat(EntityReferenceKeyUtils.toShardedKeyValue(new EntityReference("calculator", "1"), 2))
        .isEqualTo("calculator#1#2");
  }

  @Test
  void rejectsSeparatorInEntityName() {
    assertThatThrownBy(() ->
        EntityReferenceKeyUtils.toKeyValue(new EntityReference("calculator#1", "2")))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("calculator#1");
  }

  @Test
  void rejectsSeparatorInEntityId() {
    assertThatThrownBy(() ->
        EntityReferenceKeyUtils.toKeyValue(new EntityReference("calculator", "1#2")))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("1#2");
  }

  @Test
  void distinctEntitiesCannotBeFlattenedIntoOneKey() {
    assertThatThrownBy(() ->
        EntityReferenceKeyUtils.toKeyValue(new EntityReference("a#b", "c")))
        .isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() ->
        EntityReferenceKeyUtils.toKeyValue(new EntityReference("a", "b#c")))
        .isExactlyInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsSeparatorWhenSharding() {
    assertThatThrownBy(() ->
        EntityReferenceKeyUtils.toShardedKeyValue(new EntityReference("a", "b#c"), 0))
        .isExactlyInstanceOf(IllegalArgumentException.class);
  }
}
