package dev.baitursinov.caravan.test.service;

import dev.baitursinov.caravan.event.sourcing.EventSourcedRepositoryException;
import dev.baitursinov.caravan.test.event.sourcing.entity.calculator.Calculator;
import dev.baitursinov.caravan.test.event.sourcing.entity.calculator.CalculatorRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.resilience.annotation.Retryable;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class AdditionService {

  private final CalculatorRepository calculatorRepository;

  @Retryable(
      includes = EventSourcedRepositoryException.class,
      maxRetries = 100L,
      delay = 100L)
  public void addToCalculator(String calculatorId, long number) {
    var calculator = calculatorRepository.findBy(calculatorId)
        .orElseGet(() -> Calculator.createNew(calculatorId));

    calculator.addNumber(number);

    calculatorRepository.save(calculator);
  }
}
