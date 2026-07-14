package io.saga.caravan.event.sourcing;

import io.saga.caravan.event.sourcing.entity.calculator.Calculator;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EventSourcedEntityStandaloneTest {

  @Test
  void entityAppliesItsOwnEventsWithoutAnyFrameworkSetup() {
    var calculator = new Calculator(UUID.randomUUID().toString());

    calculator.addNumber(100L);
    calculator.subtractNumber(58L);

    assertThat(calculator.getCurrentNumber()).isEqualTo(42L);
    assertThat(calculator.version()).isEqualTo(2L);
  }

  @Test
  void twoIndependentEntitiesDoNotInterfere() {
    var first = new Calculator(UUID.randomUUID().toString());
    var second = new Calculator(UUID.randomUUID().toString());

    first.addNumber(10L);
    second.addNumber(20L);

    assertThat(first.getCurrentNumber()).isEqualTo(10L);
    assertThat(second.getCurrentNumber()).isEqualTo(20L);
    assertThat(first.version()).isEqualTo(1L);
    assertThat(second.version()).isEqualTo(1L);
  }
}
