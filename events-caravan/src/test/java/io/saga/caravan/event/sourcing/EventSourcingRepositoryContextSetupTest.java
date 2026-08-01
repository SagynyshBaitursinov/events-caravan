package io.saga.caravan.event.sourcing;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.EntityEventsRegistration;
import io.saga.caravan.event.EntityEventsRegistry;
import io.saga.caravan.event.Event;
import io.saga.caravan.event.producer.EventProducer;
import io.saga.caravan.event.sourcing.applying.ApplyEvent;
import io.saga.caravan.event.sourcing.applying.ApplyEventMethodPayloadsValidator;
import io.saga.caravan.event.sourcing.snapshot.EntitySnapshot;
import io.saga.caravan.event.sourcing.snapshot.SnapshotStore;
import io.saga.caravan.event.sourcing.snapshot.SnapshotTaker;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@NullMarked
class EventSourcingRepositoryContextSetupTest {

  static final String CAR = "car";
  static final String TRUCK = "truck";
  static final String TURNED_ON = "turned-on";

  record TurnedOnPayload(String reason) {
  }

  record OtherPayload(String reason) {
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

  @SuppressWarnings("SameParameterValue")
  @EntityName(TRUCK)
  static class Truck extends EventSourcedEntity {

    final String id;

    @Nullable
    String turnedOnReason;

    Truck(String id) {
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

  @SuppressWarnings("EmptyMethod")
  @EntityName(CAR)
  static class VanCalledCar extends EventSourcedEntity {

    final String id;

    VanCalledCar(String id) {
      this.id = id;
    }

    @ApplyEvent(TURNED_ON)
    void applyTurnedOn(Event<TurnedOnPayload> event) {
    }

    @Override
    public String entityId() {
      return id;
    }
  }

  @SuppressWarnings("EmptyMethod")
  @EntityName(TRUCK)
  static class WrongPayloadClassTruck extends EventSourcedEntity {

    final String id;

    WrongPayloadClassTruck(String id) {
      this.id = id;
    }

    @ApplyEvent(TURNED_ON)
    void applyTurnedOn(Event<OtherPayload> event) {
    }

    @Override
    public String entityId() {
      return id;
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

  static class TruckRepository extends EventSourcedRepository<Truck> {

    TruckRepository(EventSourcingRepositoryContext context) {
      super(Truck.class, context);
    }

    @Override
    protected Truck createWithBlankState(String entityId) {
      return new Truck(entityId);
    }
  }

  static class VanCalledCarRepository extends EventSourcedRepository<VanCalledCar> {

    VanCalledCarRepository(EventSourcingRepositoryContext context) {
      super(VanCalledCar.class, context);
    }

    @Override
    protected VanCalledCar createWithBlankState(String entityId) {
      return new VanCalledCar(entityId);
    }
  }

  static class SecondCarRepository extends EventSourcedRepository<Car> {

    SecondCarRepository(EventSourcingRepositoryContext context) {
      super(Car.class, context);
    }

    @Override
    protected Car createWithBlankState(String entityId) {
      return new Car(entityId);
    }
  }

  static class WrongPayloadClassTruckRepository extends EventSourcedRepository<WrongPayloadClassTruck> {

    WrongPayloadClassTruckRepository(EventSourcingRepositoryContext context) {
      super(WrongPayloadClassTruck.class, context);
    }

    @Override
    protected WrongPayloadClassTruck createWithBlankState(String entityId) {
      return new WrongPayloadClassTruck(entityId);
    }
  }

  record CarSnapshot(@Nullable String turnedOnReason) {
  }

  record TruckSnapshot(@Nullable String turnedOnReason) {
  }

  static class CarSnapshotTaker extends SnapshotTaker<Car, CarSnapshot> {

    CarSnapshotTaker() {
      super(Car.class, CarSnapshot.class);
    }

    @Override
    public CarSnapshot takeSnapshot(Car entity) {
      return new CarSnapshot(entity.turnedOnReason);
    }

    @Override
    public Car recreateFromSnapshot(EntityReference entityReference,
                                    CarSnapshot snapshotPayload) {
      var result = new Car(entityReference.entityId());
      result.turnedOnReason = snapshotPayload.turnedOnReason();
      return result;
    }

    @Override
    public int frequencyOfSnapshots() {
      return 1;
    }
  }

  static class SecondCarSnapshotTaker extends CarSnapshotTaker {
  }

  static class TruckSnapshotTaker extends SnapshotTaker<Truck, TruckSnapshot> {

    TruckSnapshotTaker() {
      super(Truck.class, TruckSnapshot.class);
    }

    @Override
    public TruckSnapshot takeSnapshot(Truck entity) {
      return new TruckSnapshot(entity.turnedOnReason);
    }

    @Override
    public Truck recreateFromSnapshot(EntityReference entityReference,
                                      TruckSnapshot snapshotPayload) {
      var result = new Truck(entityReference.entityId());
      result.turnedOnReason = snapshotPayload.turnedOnReason();
      return result;
    }

    @Override
    public int frequencyOfSnapshots() {
      return 1;
    }
  }

  EntityEventsRegistry entityEventsRegistry = EntityEventsRegistry.createFor(
      List.of(
          new EntityEventsRegistration(CAR, Map.of(TURNED_ON, TurnedOnPayload.class), true),
          new EntityEventsRegistration(TRUCK, Map.of(TURNED_ON, TurnedOnPayload.class), true)));

  EventStore eventStore = mock(EventStore.class);
  EventProducer eventProducer = mock(EventProducer.class);
  SnapshotStore snapshotStore = mock(SnapshotStore.class);

  EventSourcingRepositoryContext context = contextWith(List.of());

  private EventSourcingRepositoryContext contextWith(
      List<SnapshotTaker<? extends EventSourcedEntity, ?>> snapshotTakers) {

    return new EventSourcingRepositoryContext(
        eventStore,
        eventProducer,
        snapshotStore,
        new ApplyEventMethodPayloadsValidator(entityEventsRegistry),
        snapshotTakers);
  }

  @Test
  void shouldRegisterRepositoriesOfDistinctEntities() {
    assertThatCode(() -> {
      new CarRepository(context);
      new TruckRepository(context);
    }).doesNotThrowAnyException();
  }

  @Test
  void shouldPreventRegisteringEntityWithSameNameTwice() {
    new CarRepository(context);

    assertThatThrownBy(() -> new VanCalledCarRepository(context))
        .isInstanceOf(EventSourcedEntitySetupException.class)
        .hasMessage("entityName=%s or entityClass=%s are duplicated"
            .formatted(CAR, VanCalledCar.class));
  }

  @Test
  void shouldPreventRegisteringRepositoryWithSameEntityClass() {
    new CarRepository(context);

    assertThatThrownBy(() -> new SecondCarRepository(context))
        .isInstanceOf(EventSourcedEntitySetupException.class)
        .hasMessage("entityName=%s or entityClass=%s are duplicated"
            .formatted(CAR, Car.class));
  }

  @Test
  void shouldValidateRegisteredEntitiesApplyEventMethodPayloads() {
    assertThatThrownBy(() -> new WrongPayloadClassTruckRepository(context))
        .isInstanceOf(EventSourcedEntitySetupException.class)
        .hasMessage("@ApplyEvent Event parameter's payload class must be the one from EventPayloadRegistration, which is not the case for %s.applyTurnedOn"
            .formatted(WrongPayloadClassTruck.class.getName()));
  }

  @Test
  void shouldNotAllowDuplicateSnapshotTakersForSameClass() {
    var duplicatedSnapshotTakers = List.<SnapshotTaker<? extends EventSourcedEntity, ?>>of(
        new CarSnapshotTaker(),
        new SecondCarSnapshotTaker(),
        new TruckSnapshotTaker());

    assertThatThrownBy(() -> contextWith(duplicatedSnapshotTakers))
        .isInstanceOf(EventSourcedEntitySetupException.class)
        .hasMessage("Duplicate snapshot taker found for entity %s".formatted(Car.class));
  }

  @Test
  void shouldAssignCorrectSnapshotTakerToRepository() {
    var contextWithSnapshotTakers = contextWith(
        List.of(
            new CarSnapshotTaker(),
            new TruckSnapshotTaker()));

    var carRepository = new CarRepository(contextWithSnapshotTakers);
    var truckRepository = new TruckRepository(contextWithSnapshotTakers);

    var car = new Car("car-1");
    car.turnOn("ignition key");
    carRepository.save(car);

    var truck = new Truck("truck-1");
    truck.turnOn("remote start");
    truckRepository.save(truck);

    verify(snapshotStore).save(
        EntitySnapshot.<CarSnapshot>builder()
            .entityReference(new EntityReference(CAR, "car-1"))
            .version(1L)
            .payload(new CarSnapshot("ignition key"))
            .build());
    verify(snapshotStore).save(
        EntitySnapshot.<TruckSnapshot>builder()
            .entityReference(new EntityReference(TRUCK, "truck-1"))
            .version(1L)
            .payload(new TruckSnapshot("remote start"))
            .build());
    verifyNoMoreInteractions(snapshotStore);
  }

  @Test
  void shouldNotTakeSnapshotsForEntityWithoutSnapshotTaker() {
    var carRepository = new CarRepository(context);

    var car = new Car("car-1");
    car.turnOn("ignition key");
    carRepository.save(car);

    verifyNoInteractions(snapshotStore);
  }
}
