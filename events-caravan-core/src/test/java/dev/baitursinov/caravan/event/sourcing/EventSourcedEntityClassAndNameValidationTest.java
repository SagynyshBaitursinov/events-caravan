package dev.baitursinov.caravan.event.sourcing;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.EntityEventsRegistration;
import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.producer.EventProducer;
import dev.baitursinov.caravan.event.sourcing.applying.ApplyEvent;
import dev.baitursinov.caravan.event.sourcing.applying.ApplyEventMethodPayloadsValidator;
import dev.baitursinov.caravan.event.sourcing.snapshot.EntitySnapshot;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotStore;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotTaker;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@NullMarked
class EventSourcedEntityClassAndNameValidationTest {

  static final String CAR = "car";
  static final String SPORTS_CAR = "sports-car";
  static final String TURNED_ON = "turned-on";

  record TurnedOnPayload(String reason) {
  }

  record CarSnapshot(@Nullable String turnedOnReason) {
  }

  @SuppressWarnings("SameParameterValue")
  @EntityName(CAR)
  static class Car extends EventSourcedEntity {

    final String id;

    @Nullable
    String turnedOnReason;

    Car(String id) {
      this.id = id;
    }

    void turnOn(String reason) {
      recordEvent(TURNED_ON, new TurnedOnPayload(reason));
    }

    @ApplyEvent(TURNED_ON)
    void applyTurnedOn(Event<TurnedOnPayload> event) {
      this.turnedOnReason = event.payload().reason();
    }

    @Override
    public String entityId() {
      return id;
    }
  }

  @EntityName(SPORTS_CAR)
  static class SportsCar extends Car {

    SportsCar(String id) {
      super(id);
    }
  }

  static class UnannotatedCar extends Car {

    UnannotatedCar(String id) {
      super(id);
    }

    @Override
    public String entityId() {
      return "1";
    }
  }

  static class CarRepository extends EventSourcedRepository<Car> {

    CarRepository(EventSourcingRepositoryContext context) {
      super(Car.class, context);
    }

    @Override
    protected Car createWithBlankState(String entityId) {
      return new Car(entityId);
    }
  }

  static class CarRepositoryCreatingSportsCar extends EventSourcedRepository<Car> {

    CarRepositoryCreatingSportsCar(EventSourcingRepositoryContext context) {
      super(Car.class, context);
    }

    @Override
    protected Car createWithBlankState(String entityId) {
      return new SportsCar(entityId);
    }
  }

  static class CarRepositoryCreatingAnotherCarId extends EventSourcedRepository<Car> {

    CarRepositoryCreatingAnotherCarId(EventSourcingRepositoryContext context) {
      super(Car.class, context);
    }

    @Override
    protected Car createWithBlankState(String entityId) {
      return new Car("another-car-id");
    }
  }

  static class UnannotatedCarRepository extends EventSourcedRepository<UnannotatedCar> {

    UnannotatedCarRepository(EventSourcingRepositoryContext context) {
      super(UnannotatedCar.class, context);
    }

    @Override
    protected UnannotatedCar createWithBlankState(String entityId) {
      return new UnannotatedCar(entityId);
    }
  }

  static class MisbehavingCarSnapshotTaker extends SnapshotTaker<Car, CarSnapshot> {

    Function<EntityReference, Car> recreation;

    MisbehavingCarSnapshotTaker(Function<EntityReference, Car> recreation) {
      super(Car.class, CarSnapshot.class);
      this.recreation = recreation;
    }

    @Override
    public CarSnapshot takeSnapshot(Car entity) {
      return new CarSnapshot(entity.turnedOnReason);
    }

    @Override
    public Car recreateFromSnapshot(EntityReference entityReference,
                                    CarSnapshot snapshotPayload) {
      return recreation.apply(entityReference);
    }

    @Override
    public int frequencyOfSnapshots() {
      return 1;
    }
  }

  EntityEventsRegistry eventPayloadClassMap = EntityEventsRegistry.createFor(
      List.of(new EntityEventsRegistration(CAR, Map.of(TURNED_ON, TurnedOnPayload.class))));

  EventStore eventStore = mock(EventStore.class);
  EventProducer eventProducer = mock(EventProducer.class);
  SnapshotStore snapshotStore = mock(SnapshotStore.class);

  private EventSourcingRepositoryContext contextWithSnapshotTakers(
      List<SnapshotTaker<? extends EventSourcedEntity, ?>> snapshotTakers) {

    return new EventSourcingRepositoryContext(
        eventStore,
        eventProducer,
        snapshotStore,
        new ApplyEventMethodPayloadsValidator(eventPayloadClassMap),
        snapshotTakers);
  }

  private EventSourcingRepositoryContext context() {
    return contextWithSnapshotTakers(List.of());
  }

  @Nested
  class EntityNameResolution {

    @Test
    void entityAndItsRepositoryShareTheNameDeclaredOnTheEntityClass() {
      var repository = new CarRepository(context());

      assertThat(new Car("car-1").entityName()).isEqualTo(CAR);
      assertThat(repository.entityName()).isEqualTo(CAR);
      assertThat(repository.createWithBlankState("car-2").entityName()).isEqualTo(CAR);
    }

    @Test
    void entityNameIsNotInheritedBySubclasses() {
      assertThat(new SportsCar("car-1").entityName()).isEqualTo(SPORTS_CAR);
    }

    @Test
    void entityWithoutAnnotationCannotResolveItsName() {
      assertThatThrownBy(() -> new UnannotatedCar("car-1").entityName())
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("%s must declare its entityName with @EntityName"
              .formatted(UnannotatedCar.class.getName()));
    }

    @Test
    void repositoryOfEntityWithoutAnnotationCannotBeCreated() {
      assertThatThrownBy(() -> new UnannotatedCarRepository(context()))
          .isInstanceOf(EventSourcedEntitySetupException.class)
          .hasMessage("%s must declare its entityName with @EntityName"
              .formatted(UnannotatedCar.class.getName()));
    }
  }

  @Nested
  class EntityClassGuard {

    @Test
    void shouldRejectSavingAnEntityOfSubclassType() {
      var repository = new CarRepository(context());

      var sportsCar = new SportsCar("car-1");
      sportsCar.turnOn("ignition key");

      assertThatThrownBy(() -> repository.save(sportsCar))
          .isInstanceOf(EventSourcedRepositoryException.class)
          .hasMessageContaining(SportsCar.class.getName())
          .hasMessageContaining(Car.class.getName());
    }

    @Test
    void shouldRejectIfEntityIsCreatedOfSubclassType() {
      var repository = new CarRepositoryCreatingSportsCar(context());

      assertThatThrownBy(() -> repository.findBy("car-1"))
          .isInstanceOf(EventSourcedRepositoryException.class)
          .hasMessageContaining(SportsCar.class.getName());
    }

    @Test
    void shouldRejectIfEntityIsRecreatedOfSubclassType() {
      var repository = new CarRepository(
          contextWithSnapshotTakers(
              List.of(
                  new MisbehavingCarSnapshotTaker(
                      entityReference -> new SportsCar(entityReference.entityId())))));

      givenSnapshotExists();

      assertThatThrownBy(() -> repository.findBy("car-1"))
          .isInstanceOf(EventSourcedRepositoryException.class)
          .hasMessageContaining(SportsCar.class.getName());
    }

    @Test
    void shouldRejectBlankStateEntityCreatedUnderAnotherReference() {
      var repository = new CarRepositoryCreatingAnotherCarId(context());

      assertThatThrownBy(() -> repository.findBy("car-1"))
          .isInstanceOf(EventSourcedRepositoryException.class)
          .hasMessageContaining(new EntityReference(CAR, "another-car-id").toString())
          .hasMessageContaining(new EntityReference(CAR, "car-1").toString());
    }

    @Test
    void shouldRejectSnapshotEntityRecreatedUnderAnotherReference() {
      var repository = new CarRepository(
          contextWithSnapshotTakers(List.of(
              new MisbehavingCarSnapshotTaker(_ -> new Car("another-car-id")))));

      givenSnapshotExists();

      assertThatThrownBy(() -> repository.findBy("car-1"))
          .isInstanceOf(EventSourcedRepositoryException.class)
          .hasMessageContaining(new EntityReference(CAR, "another-car-id").toString())
          .hasMessageContaining(new EntityReference(CAR, "car-1").toString());
    }

    @Test
    void shouldAcceptAnEntityOfExactlyTheDeclaredClass() {
      var repository = new CarRepository(context());

      when(eventStore.getEventsOfEntity(any(), anyLong()))
          .thenReturn(Stream.of());

      var car = new Car("car-1");
      car.turnOn("ignition key");

      repository.save(car);

      assertThat(repository.findBy("car-1")).isEmpty();
    }

    private void givenSnapshotExists() {
      doReturn(Optional.of(
          EntitySnapshot.<CarSnapshot>builder()
              .entityReference(new EntityReference(CAR, "car-1"))
              .version(1L)
              .payload(new CarSnapshot("ignition key"))
              .build()))
          .when(snapshotStore).load(any(), any());
    }
  }
}
