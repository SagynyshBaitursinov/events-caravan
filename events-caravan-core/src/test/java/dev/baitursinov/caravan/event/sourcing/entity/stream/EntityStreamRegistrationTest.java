package dev.baitursinov.caravan.event.sourcing.entity.stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("DataFlowIssue")
class EntityStreamRegistrationTest {

  @Test
  void mustIncludeEntityName() {
    assertThatThrownBy(() -> new EntityStreamRegistration(null, TimeBucket.MONTHLY, 4))
        .isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  void mustIncludeTimeBucket() {
    assertThatThrownBy(() -> new EntityStreamRegistration("calculator", null, 4))
        .isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  void rejectsNonPositiveShardCount() {
    assertThatThrownBy(() -> new EntityStreamRegistration("calculator", TimeBucket.MONTHLY, 0))
        .isExactlyInstanceOf(EntityStreamRegistrationException.class)
        .hasMessageContaining("shardCount");
  }

  @Test
  void acceptsValidArguments() {
    assertThatNoException()
        .isThrownBy(() -> new EntityStreamRegistration("calculator", TimeBucket.MONTHLY, 4));
  }
}
