package dev.baitursinov.caravan.test;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.producer.EventProducer;
import dev.baitursinov.caravan.test.event.sourcing.entity.calculator.CalculatorRepository;
import dev.baitursinov.caravan.test.value.NumberAdditionRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.UUID;

import static dev.baitursinov.caravan.test.event.registration.NumberAdditionEventConfiguration.NUMBER_ADDITION_REQUEST;
import static dev.baitursinov.caravan.test.event.registration.NumberAdditionEventConfiguration.RECEIVED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ConcurrencyControlTest extends AbstractSpringBootTest {

  @Autowired
  EventProducer eventProducer;

  @Autowired
  CalculatorRepository calculatorRepository;

  @Test
  void shouldUseExternalApplierToApplyEvents() {
    var calculatorId = UUID.randomUUID().toString();

    for (int i = 0; i < 100; i++) {
      eventProducer.produce(
          Event.builder()
              .entityReference(
                  new EntityReference(NUMBER_ADDITION_REQUEST, UUID.randomUUID().toString()))
              .eventName(RECEIVED)
              .sequenceNumber(1)
              .timestamp(ZonedDateTime.now())
              .payload(new NumberAdditionRequest(calculatorId, 10L))
              .build());
    }

    await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> assertThat(calculatorRepository.findBy(calculatorId))
            .hasValueSatisfying(calculator -> assertThat(calculator.getCurrentNumber()).isEqualTo(1000L)));
  }
}
