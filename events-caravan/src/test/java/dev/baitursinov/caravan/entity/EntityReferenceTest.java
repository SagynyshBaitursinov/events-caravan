package dev.baitursinov.caravan.entity;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityReferenceTest {

  @Test
  void mustIncludeBothParts() {
    assertThatThrownBy(() -> new EntityReference("calculator", " "))
        .isExactlyInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new EntityReference(" ", "1"))
        .isExactlyInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void doesNotConstrainCharactersItsPartsAreMadeOf() {
    assertThatNoException()
        .isThrownBy(() -> new EntityReference("calculator", "1-2_3"));
  }
}
