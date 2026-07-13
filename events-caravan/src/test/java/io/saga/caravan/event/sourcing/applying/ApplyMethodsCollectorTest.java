package io.saga.caravan.event.sourcing.applying;


import io.saga.caravan.event.Event;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import io.saga.caravan.event.sourcing.EventSourcedEntitySetupException;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.BeforeEach;
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

  static class OtherPayload {
  }

  @SuppressWarnings("EmptyMethod")
  static class SimpleEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply(Event<CarTurnedOnPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "simple-entity";
    }
  }

  @SuppressWarnings("EmptyMethod")
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

    @Override
    public String entityName() {
      return "multi-event-entity";
    }
  }

  @SuppressWarnings({"unused", "EmptyMethod"})
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

    @Override
    public String entityName() {
      return "mixed-methods-entity";
    }
  }

  @SuppressWarnings("EmptyMethod")
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

    @Override
    public String entityName() {
      return "duplicate-event-entity";
    }
  }

  @SuppressWarnings("EmptyMethod")
  static abstract class ParentClass extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply(Event<CarTurnedOnPayload> event) {
    }
  }

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

    @Override
    public String entityName() {
      return "child-with-duplicate-event";
    }
  }

  @SuppressWarnings("EmptyMethod")
  static class ChildWithExtraEventEntity extends ParentClass {

    @ApplyEvent("turned-off")
    void applyTurnedOff(Event<CarTurnedOffPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "child-with-extra-event-entity";
    }
  }

  @SuppressWarnings("EmptyMethod")
  static class NoParamEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply() {
    }

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "no-param-entity";
    }
  }

  @SuppressWarnings("EmptyMethod")
  static class TwoParamEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply(Event<CarTurnedOnPayload> event, String extra) {
    }

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "two-param-entity";
    }
  }

  @SuppressWarnings("EmptyMethod")
  static class WrongParamTypeEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply(CarTurnedOnPayload payload) {
    }

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "wrong-param-type-entity";
    }
  }

  @SuppressWarnings("EmptyMethod")
  static class RawEventEntity extends EventSourcedEntity {

    @SuppressWarnings("rawtypes")
    @ApplyEvent("turned-on")
    void apply(Event event) {
    }

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "raw-event-entity";
    }
  }

  @SuppressWarnings("EmptyMethod")
  static class NotConcreteEventEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply(Event<? extends CarTurnedOnPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "raw-event-entity";
    }
  }

  @SuppressWarnings("EmptyMethod")
  static class GenericEventEntity<T> extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply(Event<T> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "raw-event-entity";
    }
  }

  @SuppressWarnings("EmptyMethod")
  static class WrongPayloadClassEntity extends EventSourcedEntity {

    @ApplyEvent("turned-on")
    void apply(Event<OtherPayload> event) {
    }

    @Override
    public String entityId() {
      return "1";
    }

    @Override
    public String entityName() {
      return "wrong-payload-class-entity";
    }
  }

  @SuppressWarnings("EmptyMethod")
  static class UnregisteredEventEntity extends EventSourcedEntity {

    @ApplyEvent("broke")
    void apply(Event<CarTurnedOnPayload> event) {
    }

    @Override
    public String entityId() {
      return "";
    }

    @Override
    public String entityName() {
      return "unregistered-event-entity";
    }
  }

  Map<EventType, Class<?>> payloadClassMap;
  ApplyMethodsCollector collector;

  @BeforeEach
  void setUp() {
    payloadClassMap = Map.of(
        new EventType("car", "turned-on"), CarTurnedOnPayload.class,
        new EventType("car", "turned-off"), CarTurnedOffPayload.class);

    collector = new ApplyMethodsCollector(payloadClassMap);
  }

  @Nested
  class HappyPath {

    @Test
    void collectsSingleAnnotatedMethod() {
      Map<EventType, Method> result =
          collector.collectApplyEventMethods(SimpleEntity.class, "car");

      assertThat(result)
          .hasSize(1)
          .containsKey(new EventType("car", "turned-on"));
    }

    @Test
    void collectedMethodMatchesTheDeclaredOne() throws NoSuchMethodException {
      Map<EventType, Method> result =
          collector.collectApplyEventMethods(SimpleEntity.class, "car");

      Method expected = SimpleEntity.class.getDeclaredMethod("apply", Event.class);
      assertThat(result.get(new EventType("car", "turned-on")))
          .isEqualTo(expected);
    }

    @Test
    void setsMethodAccessible() {
      Map<EventType, Method> result =
          collector.collectApplyEventMethods(SimpleEntity.class, "car");

      Method method = result.get(new EventType("car", "turned-on"));
      assertThat(method.canAccess(new SimpleEntity())).isTrue()
          .as("method.setAccessible(true) should have been called")
          .isTrue();
    }

    @Test
    void collectsMultipleAnnotatedMethods() {
      Map<EventType, Method> result =
          collector.collectApplyEventMethods(MultiEventEntity.class, "car");

      assertThat(result)
          .hasSize(2)
          .containsKeys(
              new EventType("car", "turned-on"),
              new EventType("car", "turned-off"));
    }

    @Test
    void ignoresNonAnnotatedMethods() {
      Map<EventType, Method> result =
          collector.collectApplyEventMethods(MixedMethodsEntity.class, "car");

      assertThat(result).hasSize(1);
    }

    @Test
    void collectsMethodsFromSuperclass() {
      Map<EventType, Method> result =
          collector.collectApplyEventMethods(ChildWithExtraEventEntity.class, "car");

      assertThat(result)
          .hasSize(2)
          .containsKeys(
              new EventType("car", "turned-on"),
              new EventType("car", "turned-off"));
    }

    @Test
    void returnsEmptyMapWhenNoAnnotatedMethodsExist() {
      Map<EventType, Method> result =
          collector.collectApplyEventMethods(EventSourcedEntity.class, "car");

      assertThat(result).isEmpty();
    }

    @Test
    void shouldOverrideParentClassApplyMethod() throws NoSuchMethodException {
      Map<EventType, Method> result = collector.collectApplyEventMethods(ChildWithOverriddenMethod.class, "car");

      Method expected = ChildWithOverriddenMethod.class.getDeclaredMethod("apply", Event.class);
      assertThat(result.get(new EventType("car", "turned-on")))
          .isEqualTo(expected);
    }
  }

  @Nested
  class Validation {

    @Test
    void shouldDetectDuplicates() {
      assertThatThrownBy(() ->
          collector.collectApplyEventMethods(DuplicateEventEntity.class, "car"))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method for EventType[entityName=car, eventName=turned-on] is duplicated");
    }

    @Test
    void throwsWhenMethodHasNoParameters() {
      assertThatThrownBy(() ->
          collector.collectApplyEventMethods(NoParamEntity.class, "car"))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method must have only one Event<PayloadClass> parameter, which is not the case for io.saga.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$NoParamEntity.apply");
    }

    @Test
    void throwsWhenMethodHasTwoParameters() {
      assertThatThrownBy(() ->
          collector.collectApplyEventMethods(TwoParamEntity.class, "car"))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method must have only one Event<PayloadClass> parameter, which is not the case for io.saga.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$TwoParamEntity.apply");
    }

    @Test
    void throwsWhenParameterIsNotEvent() {
      assertThatThrownBy(() ->
          collector.collectApplyEventMethods(WrongParamTypeEntity.class, "car"))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method must have only one Event<PayloadClass> parameter, which is not the case for io.saga.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$WrongParamTypeEntity.apply");
    }

    @Test
    void throwsWhenEventParameterIsRaw() {
      assertThatThrownBy(() ->
          collector.collectApplyEventMethods(RawEventEntity.class, "car"))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent method must have only one Event<PayloadClass> parameter, which is not the case for io.saga.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$RawEventEntity.apply");
    }

    @Test
    void throwsWhenEventParameterIsNotConcrete() {
      assertThatThrownBy(() ->
          collector.collectApplyEventMethods(NotConcreteEventEntity.class, "car"))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent Event parameter's payload class must be a concrete class, which is not the case for io.saga.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$NotConcreteEventEntity.apply");
    }

    @Test
    void throwsWhenEventParameterIsGeneric() {
      assertThatThrownBy(() ->
          collector.collectApplyEventMethods(GenericEventEntity.class, "car"))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent Event parameter's payload class must be a concrete class, which is not the case for io.saga.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$GenericEventEntity.apply");
    }

    @Test
    void throwsWhenPayloadClassDiffersFromRegistration() {
      assertThatThrownBy(() ->
          collector.collectApplyEventMethods(WrongPayloadClassEntity.class, "car"))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent Event parameter's payload class must be the one from EventPayloadRegistration, which is not the case for io.saga.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$WrongPayloadClassEntity.apply");
    }

    @Test
    void throwsWhenEventTypeIsNotInPayloadMap() {
      assertThatThrownBy(() ->
          collector.collectApplyEventMethods(UnregisteredEventEntity.class, "car"))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("@ApplyEvent Event parameter's payload class must be the one from EventPayloadRegistration, which is not the case for io.saga.caravan.event.sourcing.applying.ApplyMethodsCollectorTest$UnregisteredEventEntity.apply");
    }
  }
}