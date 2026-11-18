package dev.baitursinov.caravan.test;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.UnsequencedEventReference;
import dev.baitursinov.caravan.event.producer.EventProductionException;
import dev.baitursinov.caravan.event.producer.UnsequencedEventProducer;
import dev.baitursinov.caravan.event.sourcing.EventStore;
import dev.baitursinov.caravan.test.event.handler.TestEventRecordingEventHandler;
import dev.baitursinov.caravan.test.event.sourcing.entity.calculator.CalculatorRepository;
import dev.baitursinov.caravan.test.value.AnotherTestEventPayloadRepresentation;
import dev.baitursinov.caravan.test.value.NumberAdditionRequest;
import dev.baitursinov.caravan.test.value.TestEventPayload;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import static dev.baitursinov.caravan.test.event.registration.NumberAdditionEventConfiguration.NUMBER_ADDITION_REQUEST;
import static dev.baitursinov.caravan.test.event.registration.NumberAdditionEventConfiguration.RECEIVED;
import static dev.baitursinov.caravan.test.event.registration.TestEntityEventsConfiguration.TEST_ENTITY;
import static dev.baitursinov.caravan.test.event.registration.TestEntityEventsConfiguration.TEST_EVENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Unsequenced events going end to end: produced with {@link UnsequencedEventProducer}, published
 * straight into SNS, bypassing the event store, and consumed from SQS by the application's
 * handlers.
 */
class UnsequencedEventDrivenTest extends AbstractSpringBootTest {

  private static final Duration CONSUMPTION_TIMEOUT = Duration.ofSeconds(60);

  @Autowired
  UnsequencedEventProducer eventProducer;

  @Autowired
  EventStore eventStore;

  @Autowired
  CalculatorRepository calculatorRepository;

  @Autowired
  TestEventRecordingEventHandler testEventRecordingEventHandler;

  @Test
  void shouldDeliverUnsequencedEventToHandlersWithItsNaturalKey() {
    var testField = "unsequenced " + UUID.randomUUID();
    var event = testEntityEvent(UUID.randomUUID().toString(), testField);

    eventProducer.produce(event);

    var received = awaitReceived(testField, 1).getFirst();
    var producedReference = (UnsequencedEventReference) event.eventReference();
    assertThat(received.eventReference()).isInstanceOfSatisfying(
        UnsequencedEventReference.class,
        reference -> assertThat(reference.naturalKey()).isEqualTo(producedReference.naturalKey()));
    assertThat(received.eventReference()).isEqualTo(producedReference);
    assertThat(received.payload()).isEqualTo(event.payload());
    assertThat(received.timestamp().toInstant()).isEqualTo(event.timestamp().toInstant());
  }

  @Test
  void shouldNotWriteUnsequencedEventsIntoEventStore() {
    var testField = "not stored " + UUID.randomUUID();
    var event = testEntityEvent(UUID.randomUUID().toString(), testField);

    eventProducer.produce(event);
    awaitReceived(testField, 1);

    assertThat(eventStore.getEventsOfEntity(event.entityReference(), 0)).isEmpty();
  }

  @Test
  void shouldDeliverEveryProductionOfTheSameEventWithTheSameNaturalKey() {
    var testField = "produced twice " + UUID.randomUUID();
    var event = testEntityEvent(UUID.randomUUID().toString(), testField);

    eventProducer.produce(event);
    eventProducer.produce(event);

    var received = awaitReceived(testField, 2);
    assertThat(received)
        .extracting(Event::eventReference)
        .containsOnly(event.eventReference());
  }

  @Test
  void shouldDeliverBatchSpanningSeveralEntities() {
    var calculatorId = UUID.randomUUID().toString();
    var testField = "batch " + UUID.randomUUID();

    eventProducer.produce(List.of(
        numberAdditionRequest(calculatorId, 2L),
        testEntityEvent(UUID.randomUUID().toString(), testField),
        numberAdditionRequest(calculatorId, 3L)));

    awaitReceived(testField, 1);
    await()
        .atMost(CONSUMPTION_TIMEOUT)
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(() -> assertThat(calculatorRepository.findBy(calculatorId))
            .hasValueSatisfying(calculator -> assertThat(calculator.getCurrentNumber()).isEqualTo(5L)));
  }

  @Test
  void shouldRejectWholeBatchBeforePublishingWhenAnyEventIsInvalid() {
    var testField = "rejected " + UUID.randomUUID();
    var valid = testEntityEvent(UUID.randomUUID().toString(), testField);
    var withUnregisteredPayload = Event.unsequenced()
        .entityReference(new EntityReference(TEST_ENTITY, UUID.randomUUID().toString()))
        .eventName(TEST_EVENT)
        .naturalKey(UUID.randomUUID().toString())
        .timestamp(ZonedDateTime.now())
        .payload(new AnotherTestEventPayloadRepresentation(testField, "another"))
        .build();

    assertThatThrownBy(() -> eventProducer.produce(List.of(valid, withUnregisteredPayload)))
        .isExactlyInstanceOf(EventProductionException.class);

    await()
        .during(Duration.ofSeconds(5))
        .atMost(Duration.ofSeconds(6))
        .untilAsserted(() -> assertThat(testEventRecordingEventHandler.receivedEventsWithTestField(testField)).isEmpty());
  }

  private List<Event<TestEventPayload>> awaitReceived(String testField, int count) {
    await()
        .atMost(CONSUMPTION_TIMEOUT)
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(() -> assertThat(testEventRecordingEventHandler.receivedEventsWithTestField(testField))
            .hasSize(count));
    return testEventRecordingEventHandler.receivedEventsWithTestField(testField);
  }

  private static Event<TestEventPayload> testEntityEvent(String entityId, String testField) {
    return Event.<TestEventPayload>unsequenced()
        .entityReference(new EntityReference(TEST_ENTITY, entityId))
        .eventName(TEST_EVENT)
        .naturalKey(UUID.randomUUID().toString())
        .timestamp(ZonedDateTime.now())
        .payload(new TestEventPayload(testField, "another"))
        .build();
  }

  private static Event<NumberAdditionRequest> numberAdditionRequest(String calculatorId, long number) {
    return Event.<NumberAdditionRequest>unsequenced()
        .entityReference(new EntityReference(NUMBER_ADDITION_REQUEST, UUID.randomUUID().toString()))
        .eventName(RECEIVED)
        .naturalKey(UUID.randomUUID().toString())
        .timestamp(ZonedDateTime.now())
        .payload(new NumberAdditionRequest(calculatorId, number))
        .build();
  }
}
