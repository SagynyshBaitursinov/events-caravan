package io.saga.caravan.event.sourcing;

import io.saga.caravan.AbstractSpringBootTest;
import io.saga.caravan.event.Event;
import io.saga.caravan.event.producer.DuplicateEventProductionException;
import io.saga.caravan.event.producer.EventProductionException;
import io.saga.caravan.event.sourcing.entity.calculator.Calculator;
import io.saga.caravan.event.sourcing.entity.calculator.CalculatorRepository;
import io.saga.caravan.event.sourcing.entity.calculator.NumberCarryingEventsHandler;
import io.saga.caravan.event.sourcing.entity.calculator.NumberCarryingPayload;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class EventSourcedRepositoryTest extends AbstractSpringBootTest {

  @Autowired
  CalculatorRepository repository;

  @Autowired
  NumberCarryingEventsHandler numberCarryingEventsHandler;

  @Test
  void shouldReturnEmptyIfThereAreNoEvents() {
    Optional<Calculator> entity
        = repository.findBy(UUID.randomUUID().toString());

    assertThat(entity).isEmpty();
  }

  @Test
  void shouldNotStoreBlankStateEntity() {
    String entityId = UUID.randomUUID().toString();
    Calculator entity = new Calculator(entityId);

    assertThatThrownBy(
        () -> repository.save(entity))
        .isExactlyInstanceOf(EventSourcedRepositoryException.class);

    assertThat(repository.findBy(entityId)).isEmpty();
  }

  @Test
  void shouldSaveAndLoadEntity() {
    String entityId = UUID.randomUUID().toString();

    Calculator entity = new Calculator(entityId);
    entity.addNumber(0L);
    repository.save(entity);

    entity = repository.findBy(entityId).orElseThrow();
    assertThat(entity.getCurrentNumber()).isEqualTo(0L);

    entity.subtractNumber(10L);
    repository.save(entity);

    entity = repository.findBy(entityId).orElseThrow();
    assertThat(entity.getCurrentNumber()).isEqualTo(-10L);
  }

  @Test
  void shouldNotProhibitSavingWithoutNewEvents() {
    String entityId = UUID.randomUUID().toString();

    Calculator entity = new Calculator(entityId);
    entity.addNumber(0L);
    repository.save(entity);

    Calculator entityReferenceTwo = repository.findBy(entityId).orElseThrow();
    assertThatNoException().isThrownBy(() -> repository.save(entityReferenceTwo));
  }

  @Test
  void canSaveMoreThanOneEventAtOnce() {
    String entityId = UUID.randomUUID().toString();
    Calculator entity = new Calculator(entityId);
    entity.addNumber(1L);
    entity.subtractNumber(2L);
    entity.addNumber(5L);

    assertThatNoException().isThrownBy(() -> repository.save(entity));

    assertThat(repository.findBy(entityId))
        .hasValueSatisfying(loadedEntity ->
            assertThat(loadedEntity.getCurrentNumber())
                .isEqualTo(4L));
  }

  @Test
  void shouldBePossibleToSaveMoreThanOneEventAfterSaving() {
    String entityId = UUID.randomUUID().toString();
    Calculator entity = new Calculator(entityId);
    entity.addNumber(10L);
    repository.save(entity);
    entity.addNumber(10L);
    repository.save(entity);

    assertThat(repository.findBy(entityId))
        .hasValueSatisfying(loadedEntity ->
            assertThat(loadedEntity.getCurrentNumber())
                .isEqualTo(20));
  }

  @Test
  void canSaveMoreThanOneEventSequentially() {
    String entityId = UUID.randomUUID().toString();

    Calculator entity = new Calculator(entityId);
    entity.addNumber(1L);
    repository.save(entity);

    entity.subtractNumber(1L);
    repository.save(entity);

    assertThat(repository.findBy(entityId))
        .hasValueSatisfying(loadedEntity ->
            assertThat(loadedEntity.getCurrentNumber())
                .isEqualTo(0L));
  }

  @Test
  void savedEventShouldProduceConsumableEvent() {
    String entityId = UUID.randomUUID().toString();
    Calculator entity = new Calculator(entityId);
    entity.addNumber(3L);

    repository.save(entity);

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> {
          assertThat(numberCarryingEventsHandler.getEventsOfEntity(entityId)).hasSize(1);
          Event<NumberCarryingPayload> event = numberCarryingEventsHandler.getEventsOfEntity(entityId).getFirst();
          assertThat(event.payload().number()).isEqualTo(3L);
        });
  }

  @Test
  void shouldNotPermitParallelSavingOfOneEntity() {
    String sameId = UUID.randomUUID().toString();

    Calculator entityReferenceOne = new Calculator(sameId);
    entityReferenceOne.addNumber(1L);
    repository.save(entityReferenceOne);

    Calculator entityReferenceTwo = new Calculator(sameId);
    entityReferenceTwo.addNumber(2L);
    assertThatThrownBy(() -> repository.save(entityReferenceTwo))
        .isExactlyInstanceOf(EventSourcedRepositoryException.class)
        .hasCauseExactlyInstanceOf(DuplicateEventProductionException.class);
  }

  @Test
  void shouldNotPermitParallelModificationOfEntity() {
    String sameId = UUID.randomUUID().toString();

    Calculator entity = new Calculator(sameId);
    entity.addNumber(10L);
    repository.save(entity);

    Calculator entityReferenceOne = repository.findBy(sameId).orElseThrow();
    entityReferenceOne.subtractNumber(1);

    Calculator entityReferenceTwo = repository.findBy(sameId).orElseThrow();
    entityReferenceTwo.subtractNumber(2);

    repository.save(entityReferenceOne);

    assertThatThrownBy(() -> repository.save(entityReferenceTwo))
        .isExactlyInstanceOf(EventSourcedRepositoryException.class)
        .hasCauseExactlyInstanceOf(DuplicateEventProductionException.class);
  }

  @Test
  void shouldNotPermitDuplicateEventWithinTransaction() {
    String sameId = UUID.randomUUID().toString();

    Calculator entity = new Calculator(sameId);
    entity.addNumber(10L);
    repository.save(entity);

    Calculator entityReferenceOne = repository.findBy(sameId).orElseThrow();
    Calculator entityReferenceTwo = repository.findBy(sameId).orElseThrow();

    entityReferenceOne.subtractNumber(1L);
    entityReferenceOne.subtractNumber(2L);
    repository.save(entityReferenceOne);

    entityReferenceTwo.subtractNumber(5L);
    entityReferenceTwo.subtractNumber(6L);
    assertThatThrownBy(() -> repository.save(entityReferenceTwo))
        .isExactlyInstanceOf(EventSourcedRepositoryException.class)
        .hasCauseExactlyInstanceOf(DuplicateEventProductionException.class);

    assertThat(repository.findBy(sameId))
        .hasValueSatisfying(loadedEntity ->
            assertThat(loadedEntity.getCurrentNumber())
                .isEqualTo(7L));
  }

  @Test
  void canSaveMoreUpTo100EventsAtOnce() {
    String entityId = UUID.randomUUID().toString();
    Calculator entity = new Calculator(entityId);
    for (int i = 0; i < 100; i++) {
      entity.addNumber(1L);
    }

    assertThatNoException().isThrownBy(() -> repository.save(entity));

    assertThat(repository.findBy(entityId))
        .hasValueSatisfying(loadedEntity ->
            assertThat(loadedEntity.getCurrentNumber()).isEqualTo(100L));
  }

  @Test
  void cannotSaveMoreThan100EventsAtOnce() {
    String entityId = UUID.randomUUID().toString();
    Calculator entity = new Calculator(entityId);
    for (int i = 0; i < 101; i++) {
      entity.addNumber(1L);
    }

    assertThatThrownBy(() -> repository.save(entity))
        .isExactlyInstanceOf(EventSourcedRepositoryException.class)
        .hasCauseExactlyInstanceOf(EventProductionException.class);

    assertThat(repository.findBy(entityId)).isEmpty();
  }

  @Test
  void shouldPaginateThroughSeveralPagesOfEventDocuments() {
    String entityIdOne = UUID.randomUUID().toString();
    Calculator entityOne = new Calculator(entityIdOne);

    for (int i = 0; i < 30; i++) {
      entityOne.addNumber(1L);
      repository.save(entityOne);
    }

    assertThat(repository.findBy(entityIdOne))
        .hasValueSatisfying(loadedEntity ->
            assertThat(loadedEntity.getCurrentNumber()).isEqualTo(30L));

    String entityIdTwo = UUID.randomUUID().toString();
    Calculator entityTwo = new Calculator(entityIdTwo);

    for (int i = 0; i < 17; i++) {
      entityTwo.addNumber(1L);
      repository.save(entityTwo);
    }

    assertThat(repository.findBy(entityIdTwo))
        .hasValueSatisfying(loadedEntity ->
            assertThat(loadedEntity.getCurrentNumber()).isEqualTo(17L));
  }
}
