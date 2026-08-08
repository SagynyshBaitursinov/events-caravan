package dev.baitursinov.caravan.event.sourcing.applying;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.sourcing.EntityName;
import dev.baitursinov.caravan.event.sourcing.EventSourcedEntity;
import dev.baitursinov.caravan.event.sourcing.EventSourcedEntitySetupException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class ExternalApplyEventMethodsTest {

  @SuppressWarnings("SameParameterValue")
  @EventApplier(RobotEventApplier.class)
  @EntityName("robot")
  static class Robot extends EventSourcedEntity {

    static final String CHARGED = "charged";
    static final String DISCHARGED = "discharged";

    long currentCharge = 0;

    void charge(long amount) {
      recordEvent(CHARGED, new NumberCarryingPayload(amount));
    }

    void discharge(long amount) {
      recordEvent(DISCHARGED, new NumberCarryingPayload(amount));
    }

    @ApplyEvent(CHARGED)
    private void applyCharged(Event<NumberCarryingPayload> charged) {
      this.currentCharge += charged.payload().amount();
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  static final class RobotEventApplier {

    @ApplyEvent(Robot.DISCHARGED)
    private static void applyDischarged(Robot robot,
                                        Event<NumberCarryingPayload> discharged) {
      robot.currentCharge -= discharged.payload().amount();
    }
  }

  @EventApplier(LampEventApplier.class)
  @EntityName("lamp")
  static class Lamp extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    private void applyTurnedOn(Event<NumberCarryingPayload> turnedOn) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  static final class LampEventApplier {

    @ApplyEvent("turned-on")
    private static void applyTurnedOn(Lamp lamp,
                                      Event<NumberCarryingPayload> turnedOn) {
    }
  }

  @EventApplier({FirstFanEventApplier.class, SecondFanEventApplier.class})
  @EntityName("fan")
  static class Fan extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }
  }

  static final class FirstFanEventApplier {

    @ApplyEvent("turned-on")
    private static void applyTurnedOn(Fan fan,
                                      Event<NumberCarryingPayload> turnedOn) {
    }
  }

  static final class SecondFanEventApplier {

    @ApplyEvent("turned-on")
    private static void applyTurnedOn(Fan fan,
                                      Event<NumberCarryingPayload> turnedOn) {
    }
  }

  @EventApplier(DoorEventApplier.class)
  @EntityName("door")
  static class Door extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }
  }

  static final class DoorEventApplier {

    @ApplyEvent("opened")
    private void applyOpened(Door door,
                             Event<NumberCarryingPayload> opened) {
    }
  }

  @EventApplier(WindowEventApplier.class)
  @EntityName("window")
  static class Window extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }
  }

  static final class WindowEventApplier {

    @ApplyEvent("opened")
    private static void applyOpened(Event<NumberCarryingPayload> opened) {
    }
  }

  @EventApplier(GateEventApplier.class)
  @EntityName("gate")
  static class Gate extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }
  }

  static final class PortalEventApplier {

    @ApplyEvent("opened")
    private static void applyOpened(Event<NumberCarryingPayload> opened, Portal portal) {
    }
  }

  @EventApplier(PortalEventApplier.class)
  @EntityName("portal")
  static class Portal extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
    }
  }

  static final class GateEventApplier {

    @ApplyEvent("opened")
    private static void applyOpened(Gate opened) {
    }
  }

  @EventApplier(RobotEventApplier.class)
  @EntityName("entity-with-foreign-sources")
  static class EntityWithForeignSources extends EventSourcedEntity {

    @Override
    public String entityId() {
      return "1";
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

      Method expected = RobotEventApplier.class.getDeclaredMethod(
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
          .hasMessage("@ApplyEvent method for eventName=turned-on is duplicated between entity class dev.baitursinov.caravan.event.sourcing.applying.ExternalApplyEventMethodsTest$Lamp and its @EventApplier classes");
    }

    @Test
    void throwsWhenEventIsHandledInTwoSources() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(Fan.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method for eventName=turned-on is duplicated between @EventApplier classes of entity class dev.baitursinov.caravan.event.sourcing.applying.ExternalApplyEventMethodsTest$Fan");
    }

    @Test
    void throwsWhenExternalMethodIsNotStatic() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(Door.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method declared in  @EventApplier must be static, which is not the case for dev.baitursinov.caravan.event.sourcing.applying.ExternalApplyEventMethodsTest$DoorEventApplier.applyOpened");
    }

    @Test
    void throwsWhenExternalMethodHasNoEntityParameter() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(Window.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method declared in @EventApplier must have (Window, Event<PayloadClass>) parameters, which is not the case for dev.baitursinov.caravan.event.sourcing.applying.ExternalApplyEventMethodsTest$WindowEventApplier.applyOpened");
    }

    @Test
    void throwsWhenExternalMethodHasNoEventParameter() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(Gate.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method declared in @EventApplier must have (Gate, Event<PayloadClass>) parameters, which is not the case for dev.baitursinov.caravan.event.sourcing.applying.ExternalApplyEventMethodsTest$GateEventApplier.applyOpened");
    }

    @Test
    void throwsWhenExternalMethodHasWronglyPlacedParameter() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(Portal.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method declared in @EventApplier must have (Portal, Event<PayloadClass>) parameters, which is not the case for dev.baitursinov.caravan.event.sourcing.applying.ExternalApplyEventMethodsTest$PortalEventApplier.applyOpened");
    }

    @Test
    void throwsWhenExternalMethodHandlesAnotherEntity() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(EntityWithForeignSources.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method declared in @EventApplier must have (EntityWithForeignSources, Event<PayloadClass>) parameters, which is not the case for dev.baitursinov.caravan.event.sourcing.applying.ExternalApplyEventMethodsTest$RobotEventApplier.applyDischarged");
    }
  }
}
