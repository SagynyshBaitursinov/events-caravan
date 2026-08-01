package io.saga.caravan.event;

import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class EntityEventsRegistryTest {

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
  void shouldRejectEntityNameWithCharactersOutsideTheAllowedFormat() {
    var registrations = List.of(
        new EntityEventsRegistration("shopping cart", Map.of("added", CarTurnedOnPayload.class), true));

    assertThatThrownBy(() -> EntityEventsRegistry.createFor(registrations))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("shopping cart");
  }

  @Test
  void shouldRejectEntityNameContainingTheDynamoDbKeySeparator() {
    var registrations = List.of(
        new EntityEventsRegistration("car#1", Map.of("turned-on", CarTurnedOnPayload.class), true));

    assertThatThrownBy(() -> EntityEventsRegistry.createFor(registrations))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("car#1");
  }

  @Test
  void shouldThrowOnSameEntityAcrossMultipleRegistrations() {
    var registrations = List.of(
        new EntityEventsRegistration("car", Map.of("turned-on", CarTurnedOnPayload.class), true),
        new EntityEventsRegistration("car", Map.of("turned-off", CarTurnedOffPayload.class), true));

    assertThatThrownBy(() -> EntityEventsRegistry.createFor(registrations))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessage("Registration for entityName=car is duplicated");
  }

  @Test
  void shouldAllowSameEventNameAndPayloadOnDifferentEntities() {
    var registrations = List.of(
        new EntityEventsRegistration("car", Map.of("turned-on", CarTurnedOnPayload.class), true),
        new EntityEventsRegistration("truck", Map.of("turned-on", CarTurnedOnPayload.class), true));

    var entityEventsRegistry = EntityEventsRegistry.createFor(registrations);

    assertThat(entityEventsRegistry.payloadClassFor(new EventType("car", "turned-on")))
        .contains(CarTurnedOnPayload.class);
    assertThat(entityEventsRegistry.payloadClassFor(new EventType("truck", "turned-on")))
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

    var entityEventsRegistry = EntityEventsRegistry.createFor(registrations);

    assertThat(entityEventsRegistry.payloadClassFor(new EventType("car", "turned-on")))
        .contains(CarEventPayload.class);
    assertThat(entityEventsRegistry.payloadClassFor(new EventType("car", "turned-off")))
        .contains(CarEventPayload.class);
  }

  @Test
  void shouldReturnEmptyWhenEventTypeNotRegistered() {
    var registrations = List.of(
        new EntityEventsRegistration("car", Map.of("turned-on", CarTurnedOnPayload.class), true));

    var entityEventsRegistry = EntityEventsRegistry.createFor(registrations);

    assertThat(entityEventsRegistry.payloadClassFor(new EventType("car", "turned-off")))
        .isEmpty();
  }

  @Test
  void shouldReturnAllRegisteredEventTypes() {
    var registrations = List.of(
        new EntityEventsRegistration("car", Map.of("turned-on", CarTurnedOnPayload.class), true),
        new EntityEventsRegistration("truck", Map.of("turned-off", CarTurnedOffPayload.class), true));

    var entityEventsRegistry = EntityEventsRegistry.createFor(registrations);

    assertThat(entityEventsRegistry.registeredEventTypes())
        .containsExactlyInAnyOrder(
            new EventType("car", "turned-on"),
            new EventType("truck", "turned-off"));
  }

  @Test
  void shouldRejectAbstractClassPayload() {
    assertThatThrownBy(() -> EntityEventsRegistry.createFor(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", AbstractPayload.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class")
        .hasMessageContaining(AbstractPayload.class.getName());
  }

  @Test
  void shouldRejectInterfacePayload() {
    assertThatThrownBy(() -> EntityEventsRegistry.createFor(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", InterfacePayload.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class")
        .hasMessageContaining(InterfacePayload.class.getName());
  }

  @Test
  void shouldRejectArrayPayload() {
    assertThatThrownBy(() -> EntityEventsRegistry.createFor(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", CarTurnedOnPayload[].class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class");
  }

  @Test
  void shouldRejectPrimitivePayload() {
    assertThatThrownBy(() -> EntityEventsRegistry.createFor(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", int.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class");
  }

  @Test
  void shouldRejectNonStaticInnerClassPayload() {
    assertThatThrownBy(() -> EntityEventsRegistry.createFor(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", NonStaticInnerPayload.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class")
        .hasMessageContaining(NonStaticInnerPayload.class.getName());
  }

  @Test
  void shouldRejectAnonymousClassPayload() {
    var anonymousPayload = new Object() {
    };

    assertThatThrownBy(() -> EntityEventsRegistry.createFor(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", anonymousPayload.getClass()), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class");
  }

  @Test
  void shouldRejectLocalClassPayload() {
    class LocalPayload {
    }

    assertThatThrownBy(() -> EntityEventsRegistry.createFor(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", LocalPayload.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class")
        .hasMessageContaining(LocalPayload.class.getName());
  }

  @Test
  void shouldRejectEnumPayload() {
    assertThatThrownBy(() -> EntityEventsRegistry.createFor(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", EnumPayload.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class")
        .hasMessageContaining(EnumPayload.class.getName());
  }

  @Test
  void shouldRejectJavaBuiltInPayload() {
    assertThatThrownBy(() -> EntityEventsRegistry.createFor(
        List.of(new EntityEventsRegistration("car", Map.of("turned-on", String.class), true))))
        .isInstanceOf(EntityEventsRegistrationException.class)
        .hasMessageContaining("must be concrete class")
        .hasMessageContaining(String.class.getName());
  }
}
