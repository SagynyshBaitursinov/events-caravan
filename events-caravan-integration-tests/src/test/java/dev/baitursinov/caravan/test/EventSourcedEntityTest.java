package dev.baitursinov.caravan.test;

import dev.baitursinov.caravan.event.NoPayload;
import dev.baitursinov.caravan.event.SequencedEventReference;
import dev.baitursinov.caravan.test.event.handler.NumberCarryingEventsHandler;
import dev.baitursinov.caravan.test.event.handler.ResetEventsHandler;
import dev.baitursinov.caravan.test.event.sourcing.entity.calculator.Calculator;
import dev.baitursinov.caravan.test.event.sourcing.entity.calculator.CalculatorRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class EventSourcedEntityTest extends AbstractSpringBootTest {

  @Autowired
  CalculatorRepository repository;

  @Autowired
  NumberCarryingEventsHandler numberCarryingEventsHandler;

  @Autowired
  ResetEventsHandler resetEventsHandler;

  @Test
  void versionUpdatingTest() {
    String entityId = UUID.randomUUID().toString();
    var entity = Calculator.createNew(entityId);
    assertThat(entity.version()).isEqualTo(0L);

    entity.addNumber(101L);
    repository.save(entity);
    assertThat(entity.version()).isEqualTo(1L);

    entity.subtractNumber(102L);
    repository.save(entity);
    assertThat(entity.version()).isEqualTo(2L);

    await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() ->
            assertThat(numberCarryingEventsHandler.getEventsOfEntity(entityId))
                .hasSize(2));

    assertThat(
        numberCarryingEventsHandler.getEventsOfEntity(entityId).stream()
            .filter(event -> event.payload().number() == 101L)
            .findFirst())
        .hasValueSatisfying(
            firstEvent ->
                assertThat(firstEvent.eventReference())
                    .isInstanceOfSatisfying(SequencedEventReference.class,
                        reference -> assertThat(reference.sequenceNumber()).isEqualTo(1)));

    assertThat(
        numberCarryingEventsHandler.getEventsOfEntity(entityId).stream()
            .filter(event -> event.payload().number() == 102L)
            .findFirst())
        .hasValueSatisfying(
            firstEvent ->
                assertThat(firstEvent.eventReference())
                    .isInstanceOfSatisfying(SequencedEventReference.class,
                        reference -> assertThat(reference.sequenceNumber()).isEqualTo(2)));
  }

  @Test
  void eventWithNoPayloadIsAppliedStoredAndConsumed() {
    String entityId = UUID.randomUUID().toString();
    var entity = Calculator.createNew(entityId);

    entity.addNumber(5L);
    entity.reset();
    repository.save(entity);
    assertThat(entity.getCurrentNumber()).isZero();

    var loaded = repository.findBy(entityId).orElseThrow();
    assertThat(loaded.getCurrentNumber()).isZero();
    assertThat(loaded.version()).isEqualTo(2L);

    await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() ->
            assertThat(resetEventsHandler.getEventsOfEntity(entityId))
                .singleElement()
                .satisfies(reset -> {
                  assertThat(reset.payload()).isSameAs(NoPayload.INSTANCE);
                  assertThat(reset.eventReference())
                      .isInstanceOfSatisfying(SequencedEventReference.class,
                          reference -> assertThat(reference.sequenceNumber()).isEqualTo(2));
                }));
  }

  @Test
  void equalityAndHashCodeDependOnlyOnEntityReference() {
    String entityId = UUID.randomUUID().toString();
    var entity = Calculator.createNew(entityId);
    assertThat(entity.version()).isEqualTo(0L);

    entity.addNumber(101L);
    repository.save(entity);

    var entityReferenceTwo = repository.findBy(entityId).orElseThrow();

    assertThat(entity).isEqualTo(entityReferenceTwo);
    assertThat(entity).hasSameHashCodeAs(entityReferenceTwo);
  }
}