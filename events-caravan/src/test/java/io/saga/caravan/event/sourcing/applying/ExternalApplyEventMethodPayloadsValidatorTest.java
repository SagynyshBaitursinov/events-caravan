package io.saga.caravan.event.sourcing.applying;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import io.saga.caravan.event.sourcing.EventSourcedEntitySetupException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class ExternalApplyEventMethodPayloadsValidatorTest {

  static class CarTurnedOnPayload {
  }

  static class CarTurnedOffPayload {
  }

  static class OtherPayload {
  }

  static class ValidEntityEventApplier {
    @ApplyEvent("turned-on")
    static void applyTurnedOn(ValidEntity validEntity, Event<CarTurnedOnPayload> event) {
    }

    @ApplyEvent("turned-off")
    static void applyTurnedOff(ValidEntity validEntity, Event<CarTurnedOffPayload> event) {
    }
  }

  @EventApplier(ValidEntityEventApplier.class)
  @SuppressWarnings("EmptyMethod")
  static class ValidEntity extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "car";
    }
  }

  static class WrongPayloadClassEntityEventApplier {

    @ApplyEvent("turned-on")
    static void applyTurnedOn(WrongPayloadClassEntity wrongPayloadClassEntity, Event<OtherPayload> event) {
    }

    @ApplyEvent("turned-off")
    static void applyTurnedOff(WrongPayloadClassEntity wrongPayloadClassEntity, Event<CarTurnedOffPayload> event) {
    }
  }

  @EventApplier(WrongPayloadClassEntityEventApplier.class)
  @SuppressWarnings("EmptyMethod")
  static class WrongPayloadClassEntity extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "car";
    }
  }

  static class UnregisteredEventEntityEventApplier {

    @ApplyEvent("turned-on")
    static void applyTurnedOn(UnregisteredEventEntity unregisteredEventEntity, Event<CarTurnedOnPayload> event) {
    }

    @ApplyEvent("turned-off")
    static void applyTurnedOff(UnregisteredEventEntity unregisteredEventEntity, Event<CarTurnedOffPayload> event) {
    }

    @ApplyEvent("broke")
    static void applyBroke(UnregisteredEventEntity unregisteredEventEntity, Event<CarTurnedOnPayload> event) {
    }
  }

  @EventApplier(UnregisteredEventEntityEventApplier.class)
  @SuppressWarnings("EmptyMethod")
  static class UnregisteredEventEntity extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "car";
    }
  }

  static class MissingApplyMethodEntityEventApplier {


    @ApplyEvent("turned-on")
    static void applyTurnedOn(MissingApplyMethodEntity missingApplyMethodEntity, Event<CarTurnedOnPayload> event) {
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EventApplier(MissingApplyMethodEntityEventApplier.class)
  static class MissingApplyMethodEntity extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "car";
    }
  }

  static class StructurallyBrokenEntityEventApplier {

    @ApplyEvent("turned-on")
    static void apply1(StructurallyBrokenEntity structurallyBrokenEntity, Event<CarTurnedOnPayload> event) {
    }

    @ApplyEvent("turned-on")
    static void apply2(StructurallyBrokenEntity structurallyBrokenEntity, Event<CarTurnedOnPayload> event) {
    }

    @ApplyEvent("turned-off")
    static void applyTurnedOff(StructurallyBrokenEntity structurallyBrokenEntity, Event<CarTurnedOffPayload> event) {
    }
  }

  @EventApplier(StructurallyBrokenEntityEventApplier.class)
  @SuppressWarnings("EmptyMethod")
  static class StructurallyBrokenEntity extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "car";
    }
  }

  ApplyEventMethodPayloadsValidator applyEventMethodPayloadsValidator;
  Map<EventType, Class<?>> payloadClassMap;

  @BeforeEach
  void setUp() {
    payloadClassMap = Map.of(
        new EventType("car", "turned-on"), CarTurnedOnPayload.class,
        new EventType("car", "turned-off"), CarTurnedOffPayload.class);

    applyEventMethodPayloadsValidator = new ApplyEventMethodPayloadsValidator(payloadClassMap);
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
        .hasMessage("@ApplyEvent Event parameter's payload class must be the one from EventPayloadRegistration, which is not the case for io.saga.caravan.event.sourcing.applying.ExternalApplyEventMethodPayloadsValidatorTest$WrongPayloadClassEntityEventApplier.applyTurnedOn");
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
        .hasMessage("@ApplyEvent method for eventName=turned-on is duplicated in @EventApplier class io.saga.caravan.event.sourcing.applying.ExternalApplyEventMethodPayloadsValidatorTest$StructurallyBrokenEntityEventApplier");
  }
}
