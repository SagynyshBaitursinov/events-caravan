package io.saga.caravan.event;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityEventsRegistrationValidatorTest {

  @Test
  void acceptsNamesMadeOfAlphanumericsHyphensAndUnderscores() {
    assertThatNoException()
        .isThrownBy(() ->
            new EntityEventsRegistrationValidator(
                List.of(
                    registration("calculator"),
                    registration("test_entity"),
                    registration("Calculator2")))
                .validateAll());
  }

  @Test
  void cannotRegisterOneEntityMoreThanOnce() {
    assertThatThrownBy(() ->
        new EntityEventsRegistrationValidator(
            List.of(
                registration("calculator"),
                registration("duplicate_entity"),
                registration("duplicate_entity")))
            .validateAll())
        .isExactlyInstanceOf(EntityEventsRegistrationException.class)
        .hasMessage("Registration for entityName=duplicate_entity is duplicated");
  }

  @Test
  void rejectsNameThatIsNotAValidTopicName() {
    assertThatThrownBy(() ->
        new EntityEventsRegistrationValidator(
            List.of(registration("shopping cart")))
            .validateAll())
        .isExactlyInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("shopping cart");
  }

  @Test
  void rejectsNameThatIsTooLongForAQueueName() {
    var tooLongEntityName = "c".repeat(65);

    assertThatThrownBy(() ->
        new EntityEventsRegistrationValidator(
            List.of(registration(tooLongEntityName)))
            .validateAll())
        .isExactlyInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("64");
  }

  private EntityEventsRegistration registration(String entityName) {
    return new EntityEventsRegistration() {

      @Override
      public String entityName() {
        return entityName;
      }

      @Override
      public Map<String, Class<?>> eventToPayloadClass() {
        return Map.of();
      }

      @Override
      public boolean isSubscriptionActive() {
        return true;
      }
    };
  }
}
