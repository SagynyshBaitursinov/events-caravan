package io.saga.caravan.event;

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

  abstract static class AbstractPayload {
  }

  interface InterfacePayload {
  }

  enum EnumPayload {
    VALUE
  }

  @SuppressWarnings("InnerClassMayBeStatic")
  class NonStaticInnerPayload {
  }

  @Test
  void shouldThrowOnSameEntityAcrossMultipleRegistrations() {
    var registrations = List.of(
        new EntityEventsRegistration("car", Map.of("turned-on", CarTurnedOnPayload.class), true),
        new EntityEventsRegistration("car", Map.of("turned-off", CarTurnedOffPayload.class), true));

    assertThatThrownBy(() -> EventPayloadClassMappingKeeper.create(registrations))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessage("Registration for entityName=car is duplicated");
  }

  @Test
  void shouldAllowSameEventNameAndPayloadOnDifferentEntities() {
    var registrations = List.of(
        new EntityEventsRegistration("car", Map.of("turned-on", CarTurnedOnPayload.class), true),
        new EntityEventsRegistration("truck", Map.of("turned-on", CarTurnedOnPayload.class), true));

    var eventPayloadClassMappingKeeper = EventPayloadClassMappingKeeper.create(registrations);

    assertThat(eventPayloadClassMappingKeeper.payloadClassFor(new EventType("car", "turned-on")))
        .contains(CarTurnedOnPayload.class);
    assertThat(eventPayloadClassMappingKeeper.payloadClassFor(new EventType("truck", "turned-on")))
        .contains(CarTurnedOnPayload.class);
  }

  @Test
  void shouldAllowSamePayloadOnDifferentEventsOfSameEntity() {
    var registrations = List.of(
        new EntityEventsRegistration(
            "car",
            Map.of(
                "turned-on", CarEventPayload.class,
                "turned-off", CarEventPayload.class),
            true));

    var eventPayloadClassMappingKeeper = EventPayloadClassMappingKeeper.create(registrations);

    assertThat(eventPayloadClassMappingKeeper.payloadClassFor(new EventType("car", "turned-on")))
        .contains(CarEventPayload.class);
    assertThat(eventPayloadClassMappingKeeper.payloadClassFor(new EventType("car", "turned-off")))
        .contains(CarEventPayload.class);
  }

  @Test
  void shouldReturnEmptyWhenEventTypeNotRegistered() {
    var registrations = List.of(
        new EntityEventsRegistration("car", Map.of("turned-on", CarTurnedOnPayload.class), true));

    var eventPayloadClassMappingKeeper = EventPayloadClassMappingKeeper.create(registrations);

    assertThat(eventPayloadClassMappingKeeper.payloadClassFor(new EventType("car", "turned-off")))
        .isEmpty();
  }

  @Test
  void shouldReturnAllRegisteredEventTypes() {
    var registrations = List.of(
        new EntityEventsRegistration("car", Map.of("turned-on", CarTurnedOnPayload.class), true),
        new EntityEventsRegistration("truck", Map.of("turned-off", CarTurnedOffPayload.class), true));

    var eventPayloadClassMappingKeeper = EventPayloadClassMappingKeeper.create(registrations);

    assertThat(eventPayloadClassMappingKeeper.registeredEventTypes())
        .containsExactlyInAnyOrder(
            new EventType("car", "turned-on"),
            new EventType("truck", "turned-off"));
  }

  @Test
  void shouldRejectAbstractClassPayload() {
    assertThatThrownBy(() -> EventPayloadClassMappingKeeper.create(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", AbstractPayload.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class")
        .hasMessageContaining(AbstractPayload.class.getName());
  }

  @Test
  void shouldRejectInterfacePayload() {
    assertThatThrownBy(() -> EventPayloadClassMappingKeeper.create(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", InterfacePayload.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class")
        .hasMessageContaining(InterfacePayload.class.getName());
  }

  @Test
  void shouldRejectArrayPayload() {
    assertThatThrownBy(() -> EventPayloadClassMappingKeeper.create(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", CarTurnedOnPayload[].class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class");
  }

  @Test
  void shouldRejectPrimitivePayload() {
    assertThatThrownBy(() -> EventPayloadClassMappingKeeper.create(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", int.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class");
  }

  @Test
  void shouldRejectNonStaticInnerClassPayload() {
    assertThatThrownBy(() -> EventPayloadClassMappingKeeper.create(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", NonStaticInnerPayload.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class")
        .hasMessageContaining(NonStaticInnerPayload.class.getName());
  }

  @Test
  void shouldRejectAnonymousClassPayload() {
    var anonymousPayload = new Object() {
    };

    assertThatThrownBy(() -> EventPayloadClassMappingKeeper.create(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", anonymousPayload.getClass()), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class");
  }

  @Test
  void shouldRejectLocalClassPayload() {
    class LocalPayload {
    }

    assertThatThrownBy(() -> EventPayloadClassMappingKeeper.create(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", LocalPayload.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class")
        .hasMessageContaining(LocalPayload.class.getName());
  }

  @Test
  void shouldRejectEnumPayload() {
    assertThatThrownBy(() -> EventPayloadClassMappingKeeper.create(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", EnumPayload.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class")
        .hasMessageContaining(EnumPayload.class.getName());
  }

  @Test
  void shouldRejectJavaBuiltInPayload() {
    assertThatThrownBy(() -> EventPayloadClassMappingKeeper.create(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", String.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class")
        .hasMessageContaining(String.class.getName());
  }
}
