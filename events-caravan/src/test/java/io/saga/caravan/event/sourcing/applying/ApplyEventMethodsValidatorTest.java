package io.saga.caravan.event.sourcing.applying;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import io.saga.caravan.event.sourcing.EventSourcedEntityNamesKeeper;
import io.saga.caravan.event.sourcing.EventSourcedEntitySetupException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class ApplyEventMethodsValidatorTest {

  static class CarTurnedOnPayload {
  }

  static class CarTurnedOffPayload {
  }

  static class OtherPayload {
  }

  @SuppressWarnings("EmptyMethod")
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

    @Override
    public String entityName() {
      return "car";
    }
  }

  @SuppressWarnings("EmptyMethod")
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

    @Override
    public String entityName() {
      return "car";
    }
  }

  @SuppressWarnings("EmptyMethod")
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

    @Override
    public String entityName() {
      return "car";
    }
  }

  @SuppressWarnings("EmptyMethod")
  static class MissingApplyMethodEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void applyTurnedOn(Event<CarTurnedOnPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "car";
    }
  }

  @SuppressWarnings("EmptyMethod")
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

    @Override
    public String entityName() {
      return "car";
    }
  }

  Map<EventType, Class<?>> payloadClassMap;
  EventSourcedEntityNamesKeeper namesKeeper;

  @BeforeEach
  void setUp() {
    payloadClassMap = Map.of(
        new EventType("car", "turned-on"), CarTurnedOnPayload.class,
        new EventType("car", "turned-off"), CarTurnedOffPayload.class);

    namesKeeper = new EventSourcedEntityNamesKeeper();
  }

  private void validate(Class<? extends EventSourcedEntity> entityClass) {
    namesKeeper.register("car", entityClass);

    new ApplyEventMethodsValidator(payloadClassMap, namesKeeper)
        .afterSingletonsInstantiated();
  }

  @Test
  void passesWhenApplyMethodsMatchRegisteredEvents() {
    assertThatCode(() -> validate(ValidEntity.class))
        .doesNotThrowAnyException();
  }

  @Test
  void throwsWhenPayloadClassDiffersFromRegistration() {
    assertThatThrownBy(() -> validate(WrongPayloadClassEntity.class))
        .isInstanceOf(EventSourcedEntitySetupException.class)
        .hasMessage("@ApplyEvent Event parameter's payload class must be the one from EventPayloadRegistration, which is not the case for io.saga.caravan.event.sourcing.applying.ApplyEventMethodsValidatorTest$WrongPayloadClassEntity.applyTurnedOn");
  }

  @Test
  void throwsWhenEventTypeIsNotInPayloadMap() {
    assertThatThrownBy(() -> validate(UnregisteredEventEntity.class))
        .isInstanceOf(EventSourcedEntitySetupException.class)
        .hasMessage("@ApplyEvent Event parameter's payload class must be the one from EventPayloadRegistration, which is not the case for io.saga.caravan.event.sourcing.applying.ApplyEventMethodsValidatorTest$UnregisteredEventEntity.applyBroke");
  }

  @Test
  void throwsWhenRegisteredEventHasNoApplyMethod() {
    assertThatThrownBy(() -> validate(MissingApplyMethodEntity.class))
        .isInstanceOf(EventSourcedEntitySetupException.class)
        .hasMessage("Events with no @ApplyEvent method: [EventType[entityName=car, eventName=turned-off]]");
  }

  @Test
  void failsFastOnStructurallyBrokenEntities() {
    assertThatThrownBy(() -> validate(StructurallyBrokenEntity.class))
        .isInstanceOf(EventSourcedEntitySetupException.class)
        .hasMessage("@ApplyEvent method for eventName=turned-on is duplicated in io.saga.caravan.event.sourcing.applying.ApplyEventMethodsValidatorTest$StructurallyBrokenEntity");
  }
}
