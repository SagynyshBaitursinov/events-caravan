package dev.baitursinov.caravan.event.sourcing.applying;

import dev.baitursinov.caravan.event.EntityEventsRegistration;
import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.sourcing.EntityName;
import dev.baitursinov.caravan.event.sourcing.EventSourcedEntity;
import dev.baitursinov.caravan.event.sourcing.EventSourcedEntitySetupException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class ApplyEventMethodPayloadsValidatorTest {

  static class CarTurnedOnPayload {
  }

  static class CarTurnedOffPayload {
  }

  static class OtherPayload {
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("car")
  static class ValidEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void applyTurnedOn(Event<CarTurnedOnPayload> event) {
    }

    @ApplyEvent("turned-off")
    void applyTurnedOff(Event<CarTurnedOffPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("car")
  static class WrongPayloadClassEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void applyTurnedOn(Event<OtherPayload> event) {
    }

    @ApplyEvent("turned-off")
    void applyTurnedOff(Event<CarTurnedOffPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("car")
  static class UnregisteredEventEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void applyTurnedOn(Event<CarTurnedOnPayload> event) {
    }

    @ApplyEvent("turned-off")
    void applyTurnedOff(Event<CarTurnedOffPayload> event) {
    }

    @ApplyEvent("broke")
    void applyBroke(Event<CarTurnedOnPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("car")
  static class MissingApplyMethodEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void applyTurnedOn(Event<CarTurnedOnPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("car")
  static class StructurallyBrokenEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply1(Event<CarTurnedOnPayload> event) {
    }

    @ApplyEvent("turned-on")
    void apply2(Event<CarTurnedOnPayload> event) {
    }

    @ApplyEvent("turned-off")
    void applyTurnedOff(Event<CarTurnedOffPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  ApplyEventMethodPayloadsValidator applyEventMethodPayloadsValidator;
  EntityEventsRegistry entityEventsRegistry;

  @BeforeEach
  void setUp() {
    entityEventsRegistry = EntityEventsRegistry.createFor(
        List.of(new EntityEventsRegistration(
            "car",
            Map.of(
                "turned-on", CarTurnedOnPayload.class,
                "turned-off", CarTurnedOffPayload.class))));

    applyEventMethodPayloadsValidator = new ApplyEventMethodPayloadsValidator(entityEventsRegistry);
  }

  @Test
  void passesWhenApplyMethodsMatchRegisteredEvents() {
    assertThatCode(() -> applyEventMethodPayloadsValidator.validate("car", ValidEntity.class))
        .doesNotThrowAnyException();
  }

  @Test
  void throwsWhenPayloadClassDiffersFromRegistration() {
    assertThatThrownBy(() -> applyEventMethodPayloadsValidator.validate("car", WrongPayloadClassEntity.class))
        .isInstanceOf(EventSourcedEntitySetupException.class)
        .hasMessage("@ApplyEvent Event parameter's payload class must be of type dev.baitursinov.caravan.event.sourcing.applying.ApplyEventMethodPayloadsValidatorTest$CarTurnedOnPayload, which is not the case for dev.baitursinov.caravan.event.sourcing.applying.ApplyEventMethodPayloadsValidatorTest$WrongPayloadClassEntity.applyTurnedOn");
  }

  @Test
  void throwsWhenEventTypeIsNotInPayloadMap() {
    assertThatThrownBy(() -> applyEventMethodPayloadsValidator.validate("car", UnregisteredEventEntity.class))
        .isInstanceOf(EventSourcedEntitySetupException.class)
        .hasMessage("There's no registered event payload class for EventType[entityName=car, eventName=broke]");
  }

  @Test
  void throwsWhenEntityNameIsWrong() {
    assertThatThrownBy(() -> applyEventMethodPayloadsValidator.validate("auto", ValidEntity.class))
        .isInstanceOf(EventSourcedEntitySetupException.class)
        .hasMessage("There's no registered event payload class for EventType[entityName=auto, eventName=turned-on]");
  }

  @Test
  void throwsWhenRegisteredEventHasNoApplyMethod() {
    assertThatThrownBy(() -> applyEventMethodPayloadsValidator.validate("car", MissingApplyMethodEntity.class))
        .isInstanceOf(EventSourcedEntitySetupException.class)
        .hasMessage("Events with no @ApplyEvent method: [EventType[entityName=car, eventName=turned-off]]");
  }

  @Test
  void failsFastOnStructurallyBrokenEntities() {
    assertThatThrownBy(() -> applyEventMethodPayloadsValidator.validate("car", StructurallyBrokenEntity.class))
        .isInstanceOf(EventSourcedEntitySetupException.class)
        .hasMessage("@ApplyEvent method for eventName=turned-on is duplicated in entity class dev.baitursinov.caravan.event.sourcing.applying.ApplyEventMethodPayloadsValidatorTest$StructurallyBrokenEntity");
  }
}
