package io.saga.caravan.test.event.sourcing;

import io.saga.caravan.test.AbstractSpringBootTest;
import io.saga.caravan.test.event.handler.NumberCarryingEventsHandler;
import io.saga.caravan.test.event.sourcing.entity.calculator.Calculator;
import io.saga.caravan.test.event.sourcing.entity.calculator.CalculatorRepository;
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
        .atMost(Duration.ofSeconds(15))
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
                assertThat(firstEvent.sequenceNumber()).isEqualTo(1));

    assertThat(
        numberCarryingEventsHandler.getEventsOfEntity(entityId).stream()
            .filter(event -> event.payload().number() == 102L)
            .findFirst())
        .hasValueSatisfying(
            firstEvent ->
                assertThat(firstEvent.sequenceNumber()).isEqualTo(2));
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