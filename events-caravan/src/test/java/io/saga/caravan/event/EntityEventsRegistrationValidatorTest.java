package io.saga.caravan.event;

import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class EntityEventsRegistrationValidatorTest {

  @Test
  void acceptsNamesMadeOfAlphanumericsHyphensAndUnderscores() {
    assertThatNoException()
        .isThrownBy(() ->
            new EntityEventsRegistrationValidator(
                List.of(
                    new EntityEventsRegistration("calculator", Map.of(), true),
                    new EntityEventsRegistration("test_entity", Map.of(), true),
                    new EntityEventsRegistration("Calculator2", Map.of(), true)))
                .validateAll());
  }

  @Test
  void rejectsNameThatIsNotAValidTopicName() {
    assertThatThrownBy(() ->
        new EntityEventsRegistrationValidator(
            List.of(new EntityEventsRegistration("shopping cart", Map.of(), true)))
            .validateAll())
        .isExactlyInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("shopping cart");
  }

  @Test
  void rejectsNameThatIsTooLongForAQueueName() {
    var tooLongEntityName = "c".repeat(65);

    assertThatThrownBy(() ->
        new EntityEventsRegistrationValidator(
            List.of(new EntityEventsRegistration(tooLongEntityName, Map.of(), true)))
            .validateAll())
        .isExactlyInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("64");
  }
}
