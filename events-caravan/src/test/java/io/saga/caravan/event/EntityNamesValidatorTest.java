package io.saga.caravan.event;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityNamesValidatorTest {

  @Test
  void acceptsNamesMadeOfAlphanumericsHyphensAndUnderscores() {
    assertThatNoException()
        .isThrownBy(() ->
            new EntityNamesValidator(
                List.of(
                    registration("snapshotting-calculator"),
                    registration("test_entity"),
                    registration("Calculator2")))
                .afterSingletonsInstantiated());
  }

  @Test
  void rejectsNameThatIsNotAValidTopicName() {
    assertThatThrownBy(() ->
        new EntityNamesValidator(
            List.of(registration("shopping cart")))
            .afterSingletonsInstantiated())
        .isExactlyInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("shopping cart");
  }

  @Test
  void rejectsNameThatIsTooLongForAQueueName() {
    var tooLongEntityName = "c".repeat(65);

    assertThatThrownBy(() ->
        new EntityNamesValidator(
            List.of(registration(tooLongEntityName)))
            .afterSingletonsInstantiated())
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
