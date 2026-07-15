package io.saga.caravan.event.sourcing.applying;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import io.saga.caravan.event.sourcing.EventSourcedEntityNamesKeeper;
import io.saga.caravan.event.sourcing.EventSourcedEntitySetupException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class ExternalApplyEventMethodsTest {

  record ChargePayload(long amount) {
  }

  @SuppressWarnings("SameParameterValue")
  @EventApplier(RobotEventAppliers.class)
  static class Robot extends EventSourcedEntity {

    static final String CHARGED = "charged";
    static final String DISCHARGED = "discharged";

    long currentCharge = 0;

    void charge(long amount) {
      recordEvent(CHARGED, new ChargePayload(amount));
    }

    void discharge(long amount) {
      recordEvent(DISCHARGED, new ChargePayload(amount));
    }

    @ApplyEvent(CHARGED)
    private void applyCharged(Event<ChargePayload> charged) {
      this.currentCharge += charged.payload().amount();
    }

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "robot";
    }
  }

  static final class RobotEventAppliers {

    @ApplyEvent(Robot.DISCHARGED)
    private static void applyDischarged(Robot robot,
                                        Event<ChargePayload> discharged) {
      robot.currentCharge -= discharged.payload().amount();
    }
  }

  @EventApplier(LampEventAppliers.class)
  static class Lamp extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    private void applyTurnedOn(Event<ChargePayload> turnedOn) {
    }

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "lamp";
    }
  }

  static final class LampEventAppliers {

    @ApplyEvent("turned-on")
    private static void applyTurnedOn(Lamp lamp,
                                      Event<ChargePayload> turnedOn) {
    }
  }

  @EventApplier({FirstFanEventAppliers.class, SecondFanEventAppliers.class})
  static class Fan extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "fan";
    }
  }

  static final class FirstFanEventAppliers {

    @ApplyEvent("turned-on")
    private static void applyTurnedOn(Fan fan,
                                      Event<ChargePayload> turnedOn) {
    }
  }

  static final class SecondFanEventAppliers {

    @ApplyEvent("turned-on")
    private static void applyTurnedOn(Fan fan,
                                      Event<ChargePayload> turnedOn) {
    }
  }

  @EventApplier(DoorEventAppliers.class)
  static class Door extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "door";
    }
  }

  static final class DoorEventAppliers {

    @ApplyEvent("opened")
    private void applyOpened(Door door,
                             Event<ChargePayload> opened) {
    }
  }

  @EventApplier(WindowEventAppliers.class)
  static class Window extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "window";
    }
  }

  static final class WindowEventAppliers {

    @ApplyEvent("opened")
    private static void applyOpened(Event<ChargePayload> opened) {
    }
  }

  @EventApplier(RobotEventAppliers.class)
  static class EntityWithForeignSources extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "entity-with-foreign-sources";
    }
  }

  @Nested
  class HappyPath {

    @Test
    void collectsMethodsFromBothEntityClassAndItsSources() {
      Map<String, Method> result =
          ApplyMethodsCollector.applyEventMethodsOf(Robot.class);

      assertThat(result)
          .hasSize(2)
          .containsKeys(Robot.CHARGED, Robot.DISCHARGED);
    }

    @Test
    void collectedExternalMethodMatchesTheDeclaredOne() throws NoSuchMethodException {
      Map<String, Method> result =
          ApplyMethodsCollector.applyEventMethodsOf(Robot.class);

      Method expected = RobotEventAppliers.class.getDeclaredMethod(
          "applyDischarged", Robot.class, Event.class);
      assertThat(result.get(Robot.DISCHARGED))
          .isEqualTo(expected);
    }

    @Test
    void appliesEventsThroughExternalMethod() {
      var robot = new Robot();

      robot.charge(10);
      robot.discharge(4);

      assertThat(robot.currentCharge).isEqualTo(6);
      assertThat(robot.version()).isEqualTo(2);
    }
  }

  @Nested
  class Validation {

    @Test
    void throwsWhenEventIsHandledInBothEntityClassAndItsSources() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(Lamp.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method for eventName=turned-on is duplicated between entity class io.saga.caravan.event.sourcing.applying.ExternalApplyEventMethodsTest$Lamp and its @EventApplier classes");
    }

    @Test
    void throwsWhenEventIsHandledInTwoSources() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(Fan.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method for eventName=turned-on is duplicated between @EventApplier classes of entity class io.saga.caravan.event.sourcing.applying.ExternalApplyEventMethodsTest$Fan");
    }

    @Test
    void throwsWhenExternalMethodIsNotStatic() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(Door.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method declared in  @EventApplier must be static, which is not the case for io.saga.caravan.event.sourcing.applying.ExternalApplyEventMethodsTest$DoorEventAppliers.applyOpened");
    }

    @Test
    void throwsWhenExternalMethodHasNoEntityParameter() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(Window.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method declared in @EventApplier must have (Window, Event<PayloadClass>) parameters, which is not the case for io.saga.caravan.event.sourcing.applying.ExternalApplyEventMethodsTest$WindowEventAppliers.applyOpened");
    }

    @Test
    void throwsWhenExternalMethodHandlesAnotherEntity() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(EntityWithForeignSources.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method declared in @EventApplier must have (EntityWithForeignSources, Event<PayloadClass>) parameters, which is not the case for io.saga.caravan.event.sourcing.applying.ExternalApplyEventMethodsTest$RobotEventAppliers.applyDischarged");
    }
  }

  @Nested
  class PayloadClassValidation {

    private void validate(Class<?> dischargedPayloadClass) {
      Map<EventType, Class<?>> payloadClassMap = Map.of(
          new EventType("robot", Robot.CHARGED), ChargePayload.class,
          new EventType("robot", Robot.DISCHARGED), dischargedPayloadClass);

      var namesKeeper = new EventSourcedEntityNamesKeeper();
      namesKeeper.register("robot", Robot.class);

      new ApplyEventMethodsValidator(payloadClassMap, namesKeeper)
          .afterSingletonsInstantiated();
    }

    @Test
    void validatesPayloadClassOfExternalMethods() {
      assertThatCode(() -> validate(ChargePayload.class))
          .doesNotThrowAnyException();
    }

    @Test
    void throwsWhenExternalMethodPayloadClassDiffersFromRegistration() {
      assertThatThrownBy(() -> validate(String.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent Event parameter's payload class must be the one from EventPayloadRegistration, which is not the case for io.saga.caravan.event.sourcing.applying.ExternalApplyEventMethodsTest$RobotEventAppliers.applyDischarged");
    }
  }
}
