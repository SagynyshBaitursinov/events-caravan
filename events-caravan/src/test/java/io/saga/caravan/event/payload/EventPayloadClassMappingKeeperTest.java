package io.saga.caravan.event.payload;

import io.saga.caravan.event.EntityEventsRegistration;
import io.saga.caravan.event.EntityEventsRegistrationException;
import io.saga.caravan.event.EventPayloadClassMappingKeeper;
import io.saga.caravan.event.EventType;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class EventPayloadClassMappingKeeperTest {

  static class CarTurnedOnPayload {
  }

  static class CarTurnedOffPayload {
  }

  static class CarEventPayload {
  }

  @Test
  void shouldThrowOnDuplicateEventTypeRegistration() {
    var registrations = List.of(
        registration("car", Map.of("turned-on", CarTurnedOnPayload.class)),
        registration("car", Map.of("turned-on", CarTurnedOffPayload.class)));

    assertThatThrownBy(() -> EventPayloadClassMappingKeeper.create(registrations))
        .isInstanceOf(EntityEventsRegistrationException.class)
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

    var eventPayloadClassMappingKeeper = EventPayloadClassMappingKeeper.create(registrations);

    assertThat(eventPayloadClassMappingKeeper.payloadClassFor(new EventType("car", "turned-on")))
        .contains(CarTurnedOnPayload.class);
    assertThat(eventPayloadClassMappingKeeper.payloadClassFor(new EventType("car", "turned-off")))
        .contains(CarTurnedOffPayload.class);
  }

  @Test
  void shouldAllowSameEventNameAndPayloadOnDifferentEntities() {
    var registrations = List.of(
        registration("car", Map.of("turned-on", CarTurnedOnPayload.class)),
        registration("truck", Map.of("turned-on", CarTurnedOnPayload.class)));

    var eventPayloadClassMappingKeeper = EventPayloadClassMappingKeeper.create(registrations);

    assertThat(eventPayloadClassMappingKeeper.payloadClassFor(new EventType("car", "turned-on")))
        .contains(CarTurnedOnPayload.class);
    assertThat(eventPayloadClassMappingKeeper.payloadClassFor(new EventType("truck", "turned-on")))
        .contains(CarTurnedOnPayload.class);
  }

  @Test
  void shouldAllowSamePayloadOnDifferentEventsOfSameEntity() {
    var registrations = List.of(
        registration("car",
            Map.of(
                "turned-on", CarEventPayload.class,
                "turned-off", CarEventPayload.class)));

    var eventPayloadClassMappingKeeper = EventPayloadClassMappingKeeper.create(registrations);

    assertThat(eventPayloadClassMappingKeeper.payloadClassFor(new EventType("car", "turned-on")))
        .contains(CarEventPayload.class);
    assertThat(eventPayloadClassMappingKeeper.payloadClassFor(new EventType("car", "turned-off")))
        .contains(CarEventPayload.class);
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
