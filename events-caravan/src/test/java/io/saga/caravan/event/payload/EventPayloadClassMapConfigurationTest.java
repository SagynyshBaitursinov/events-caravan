package io.saga.caravan.event.payload;

import io.saga.caravan.event.EntityEventsRegistration;
import io.saga.caravan.event.EventType;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class EventPayloadClassMapConfigurationTest {

  static class CarTurnedOnPayload {
  }

  static class CarTurnedOffPayload {
  }

  private final EventPayloadClassMapConfiguration configuration
      = new EventPayloadClassMapConfiguration();

  @Test
  void shouldThrowOnDuplicateEventTypeRegistration() {
    var registrations = List.of(
        registration("car", Map.of("turned-on", CarTurnedOnPayload.class)),
        registration("car", Map.of("turned-on", CarTurnedOffPayload.class)));

    assertThatThrownBy(() -> configuration.eventPayloadClassMap(registrations))
        .isInstanceOf(EventPayloadRegistrationException.class)
        .hasMessageContaining("car")
        .hasMessageContaining("turned-on")
        .hasMessageContaining(CarTurnedOnPayload.class.getName())
        .hasMessageContaining(CarTurnedOffPayload.class.getName());
  }

  @Test
  void shouldAllowSameEntityAcrossRegistrationsWithDistinctEvents() {
    var registrations = List.of(
        registration("car", Map.of("turned-on", CarTurnedOnPayload.class)),
        registration("car", Map.of("turned-off", CarTurnedOffPayload.class)));

    var eventPayloadClassMap = configuration.eventPayloadClassMap(registrations);

    assertThat(eventPayloadClassMap)
        .containsEntry(new EventType("car", "turned-on"), CarTurnedOnPayload.class)
        .containsEntry(new EventType("car", "turned-off"), CarTurnedOffPayload.class);
  }

  @Test
  void shouldAllowSameEventNameOnDifferentEntities() {
    var registrations = List.of(
        registration("car", Map.of("turned-on", CarTurnedOnPayload.class)),
        registration("truck", Map.of("turned-on", CarTurnedOnPayload.class)));

    var eventPayloadClassMap = configuration.eventPayloadClassMap(registrations);

    assertThat(eventPayloadClassMap)
        .containsEntry(new EventType("car", "turned-on"), CarTurnedOnPayload.class)
        .containsEntry(new EventType("truck", "turned-on"), CarTurnedOnPayload.class);
  }

  private EntityEventsRegistration registration(String entityName,
                                                Map<String, Class<?>> eventToPayloadClass) {
    return new EntityEventsRegistration() {

      @Override
      public String entityName() {
        return entityName;
      }

      @Override
      public Map<String, Class<?>> eventToPayloadClass() {
        return eventToPayloadClass;
      }

      @Override
      public boolean isSubscriptionActive() {
        return true;
      }
    };
  }
}
