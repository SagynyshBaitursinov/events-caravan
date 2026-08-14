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
class ApplyMethodsCollectorTest {

  static class CarTurnedOnPayload {
  }

  static class CarTurnedOffPayload {
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("simple-entity")
  static class SimpleEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    private void apply(Event<CarTurnedOnPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("multi-event-entity")
  static class MultiEventEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void applyCreated(Event<CarTurnedOnPayload> event) {
    }

    @ApplyEvent("turned-off")
    void applyCancelled(Event<CarTurnedOffPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings({"unused", "EmptyMethod"})
  @EntityName("mixed-methods-entity")
  static class MixedMethodsEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply(Event<CarTurnedOnPayload> event) {
    }

    void notAnnotated(Event<CarTurnedOnPayload> event) {
    }

    void alsoNotAnnotated() {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("duplicate-event-entity")
  static class DuplicateEventEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply1(Event<CarTurnedOnPayload> event) {
    }

    @ApplyEvent("turned-on")
    void apply2(Event<CarTurnedOnPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  static abstract class ParentClass extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply(Event<CarTurnedOnPayload> event) {
    }
  }

  @EntityName("child-with-duplicate-event")
  static class ChildWithOverriddenMethod extends ParentClass {

    @SuppressWarnings("RedundantMethodOverride")
    @Override
    @ApplyEvent("turned-on")
    void apply(Event<CarTurnedOnPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("child-with-extra-event-entity")
  static class ChildWithExtraEventEntity extends ParentClass {

    @ApplyEvent("turned-off")
    void applyTurnedOff(Event<CarTurnedOffPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("no-param-entity")
  static class NoParamEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply() {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("two-param-entity")
  static class TwoParamEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply(Event<CarTurnedOnPayload> event, String extra) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("wrong-param-type-entity")
  static class WrongParamTypeEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply(CarTurnedOnPayload payload) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("raw-event-entity")
  static class RawEventEntity extends EventSourcedEntity {

    @SuppressWarnings("rawtypes")
    @ApplyEvent("turned-on")
    void apply(Event event) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("not-concrete-event-entity")
  static class NotConcreteEventEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply(Event<? extends CarTurnedOnPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName("generic-event-entity")
  static class GenericEventEntity<T> extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply(Event<T> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @SuppressWarnings("SameParameterValue")
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

    @ApplyEvent(ExternalApplyEventMethodsTest.Robot.DISCHARGED)
    private void applyDischarged(Event<NumberCarryingPayload> discharged) {
      this.currentCharge -= discharged.payload().amount();
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  @Nested
  class HappyPath {

    @Test
    void collectsSingleAnnotatedMethod() {
      Map<String, Method> result =
          ApplyMethodsCollector.applyEventMethodsOf(SimpleEntity.class);

      assertThat(result)
          .hasSize(1)
          .containsKey("turned-on");
    }

    @Test
    void collectedMethodMatchesTheDeclaredOne() throws NoSuchMethodException {
      Map<String, Method> result =
          ApplyMethodsCollector.applyEventMethodsOf(SimpleEntity.class);

      Method expected = SimpleEntity.class.getDeclaredMethod("apply", Event.class);
      assertThat(result.get("turned-on"))
          .isEqualTo(expected);
    }

    @Test
    void setsMethodAccessible() {
      Map<String, Method> result =
          ApplyMethodsCollector.applyEventMethodsOf(SimpleEntity.class);

      Method method = result.get("turned-on");
      assertThat(method.canAccess(new SimpleEntity()))
          .as("method.setAccessible(true) should have been called")
          .isTrue();
    }

    @Test
    void collectsMultipleAnnotatedMethods() {
      Map<String, Method> result =
          ApplyMethodsCollector.applyEventMethodsOf(MultiEventEntity.class);

      assertThat(result)
          .hasSize(2)
          .containsKeys("turned-on", "turned-off");
    }

    @Test
    void ignoresNonAnnotatedMethods() {
      Map<String, Method> result =
          ApplyMethodsCollector.applyEventMethodsOf(MixedMethodsEntity.class);

      assertThat(result).hasSize(1);
    }

    @Test
    void collectsMethodsFromSuperclass() {
      Map<String, Method> result =
          ApplyMethodsCollector.applyEventMethodsOf(ChildWithExtraEventEntity.class);

      assertThat(result)
          .hasSize(2)
          .containsKeys("turned-on", "turned-off");
    }

    @Test
    void returnsEmptyMapWhenNoAnnotatedMethodsExist() {
      Map<String, Method> result =
          ApplyMethodsCollector.applyEventMethodsOf(EventSourcedEntity.class);

      assertThat(result).isEmpty();
    }

    @Test
    void shouldOverrideParentClassApplyMethod() throws NoSuchMethodException {
      Map<String, Method> result =
          ApplyMethodsCollector.applyEventMethodsOf(ChildWithOverriddenMethod.class);

      Method expected = ChildWithOverriddenMethod.class.getDeclaredMethod("apply", Event.class);
      assertThat(result.get("turned-on"))
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
    void shouldDetectDuplicates() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(DuplicateEventEntity.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method for eventName=turned-on is duplicated in entity class dev.baitursinov.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$DuplicateEventEntity");
    }

    @Test
    void throwsWhenMethodHasNoParameters() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(NoParamEntity.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method must have only one Event<PayloadClass> parameter, which is not the case for dev.baitursinov.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$NoParamEntity.apply");
    }

    @Test
    void throwsWhenMethodHasTwoParameters() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(TwoParamEntity.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method must have only one Event<PayloadClass> parameter, which is not the case for dev.baitursinov.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$TwoParamEntity.apply");
    }

    @Test
    void throwsWhenParameterIsNotEvent() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(WrongParamTypeEntity.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method must have only one Event<PayloadClass> parameter, which is not the case for dev.baitursinov.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$WrongParamTypeEntity.apply");
    }

    @Test
    void throwsWhenEventParameterIsRaw() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(RawEventEntity.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method must have only one Event<PayloadClass> parameter, which is not the case for dev.baitursinov.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$RawEventEntity.apply");
    }

    @Test
    void throwsWhenEventParameterIsNotConcrete() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(NotConcreteEventEntity.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent Event parameter's payload class must be a concrete class, which is not the case for dev.baitursinov.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$NotConcreteEventEntity.apply");
    }

    @Test
    void throwsWhenEventParameterIsGeneric() {
      assertThatThrownBy(() ->
          ApplyMethodsCollector.applyEventMethodsOf(GenericEventEntity.class))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent Event parameter's payload class must be a concrete class, which is not the case for dev.baitursinov.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$GenericEventEntity.apply");
    }
  }
}
