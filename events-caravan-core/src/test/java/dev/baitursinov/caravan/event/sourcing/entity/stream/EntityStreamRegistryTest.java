package dev.baitursinov.caravan.event.sourcing.entity.stream;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityStreamRegistryTest {

  @Test
  void shouldThrowOnSameEntityAcrossMultipleRegistrations() {
    var registrations = List.of(
        new EntityStreamRegistration("calculator", TimeBucket.MONTHLY, 4),
        new EntityStreamRegistration("calculator", TimeBucket.DAILY, 8));

    assertThatThrownBy(() -> EntityStreamRegistry.createFor(registrations))
        .isInstanceOf(EntityStreamRegistrationException.class)
        .hasMessage("Registration for entityName=calculator is duplicated");
  }

  @Test
  void returnsRegistrationForRegisteredEntityName() {
    var registration = new EntityStreamRegistration("calculator", TimeBucket.MONTHLY, 4);
    var registry = EntityStreamRegistry.createFor(List.of(registration));

    assertThat(registry.registrationFor("calculator")).contains(registration);
  }

  @Test
  void returnsEmptyForUnregisteredEntityName() {
    var registry = EntityStreamRegistry.createFor(
        List.of(new EntityStreamRegistration("calculator", TimeBucket.MONTHLY, 4)));

    assertThat(registry.registrationFor("car")).isEmpty();
  }
}
