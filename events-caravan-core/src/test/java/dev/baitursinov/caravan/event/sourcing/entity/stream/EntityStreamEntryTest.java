package dev.baitursinov.caravan.event.sourcing.entity.stream;

import dev.baitursinov.caravan.entity.EntityReference;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("DataFlowIssue")
class EntityStreamEntryTest {

  private static final EntityReference ENTITY_REFERENCE = new EntityReference("calculator", "1");
  private static final ZonedDateTime TIMESTAMP = ZonedDateTime.now();

  @Test
  void mustIncludeEntityReference() {
    assertThatThrownBy(() -> new EntityStreamEntry(null, TIMESTAMP))
        .isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  void mustIncludeFirstEventTimestamp() {
    assertThatThrownBy(() -> new EntityStreamEntry(ENTITY_REFERENCE, null))
        .isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  void acceptsValidArguments() {
    assertThatNoException()
        .isThrownBy(() -> new EntityStreamEntry(ENTITY_REFERENCE, TIMESTAMP));
  }
}
