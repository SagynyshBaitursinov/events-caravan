package dev.baitursinov.caravan.test.event.sourcing.snapshotting;

import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotStore;
import dev.baitursinov.caravan.test.AbstractSpringBootTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class EntitySnapshottingTest extends AbstractSpringBootTest {

  @Autowired
  SnapshottingCalculatorRepository repository;

  @Autowired
  SnapshotStore snapshotStore;

  @Test
  void shouldNotCreateSnapshotIfDidNotReachThreshold() {
    String entityId = UUID.randomUUID().toString();

    var calculator = new SnapshottingCalculator(entityId);
    for (int i = 0; i < 4; i++) {
      calculator = findOrCreate(entityId);
      addOneAndSave(calculator);
    }

    calculator = findOrCreate(entityId);
    assertThat(calculator.getCurrentNumber()).isEqualTo(4);
    assertThat(calculator.version()).isEqualTo(4);

    assertThat(snapshotStore.load(calculator.entityReference(), CalculatorSnapshot.class))
        .isEmpty();
  }

  @ParameterizedTest
  @CsvSource({
      "8,5",
      "10,10",
      "16,15"
  })
  void shouldCalculateCorrectlyAfterSnapshottingThresholdIsPassed(int numberOfAdditions,
                                                                  int expectedLastVersionNumber) {
    String entityId = UUID.randomUUID().toString();

    var calculator = new SnapshottingCalculator(entityId);
    for (int i = 0; i < numberOfAdditions; i++) {
      calculator = findOrCreate(entityId);
      addOneAndSave(calculator);
    }

    calculator = findOrCreate(entityId);
    assertThat(calculator.getCurrentNumber()).isEqualTo(numberOfAdditions);
    assertThat(calculator.version()).isEqualTo(numberOfAdditions);

    assertThat(snapshotStore.load(calculator.entityReference(), CalculatorSnapshot.class))
        .hasValueSatisfying(snapshot -> {
          assertThat(snapshot.version()).isEqualTo(expectedLastVersionNumber);
          assertThat(snapshot.payload().currentNumber()).isEqualTo(expectedLastVersionNumber);
        });
  }

  private SnapshottingCalculator findOrCreate(String entityId) {
    return repository
        .findBy(entityId)
        .orElseGet(() -> new SnapshottingCalculator(entityId));
  }

  private void addOneAndSave(SnapshottingCalculator entity) {
    entity.addNumber(1L);
    repository.save(entity);
  }
}
