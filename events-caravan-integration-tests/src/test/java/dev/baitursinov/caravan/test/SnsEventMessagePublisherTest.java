package dev.baitursinov.caravan.test;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.EventReference;
import dev.baitursinov.caravan.event.publisher.EventMessage;
import dev.baitursinov.caravan.event.publisher.EventMessagePublisher;
import dev.baitursinov.caravan.event.publisher.EventPublicationException;
import dev.baitursinov.caravan.event.serialization.EventPayloadSerializer;
import dev.baitursinov.caravan.event.serialization.EventSerializer;
import dev.baitursinov.caravan.test.event.handler.AllTestFieldSavingEventHandler;
import dev.baitursinov.caravan.test.event.sourcing.entity.calculator.CalculatorRepository;
import dev.baitursinov.caravan.test.value.NumberAdditionRequest;
import dev.baitursinov.caravan.test.value.TestEventPayload;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static dev.baitursinov.caravan.test.event.registration.NumberAdditionEventConfiguration.NUMBER_ADDITION_REQUEST;
import static dev.baitursinov.caravan.test.event.registration.NumberAdditionEventConfiguration.RECEIVED;
import static dev.baitursinov.caravan.test.event.registration.TestEntityEventsConfiguration.TEST_ENTITY;
import static dev.baitursinov.caravan.test.event.registration.TestEntityEventsConfiguration.TEST_EVENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class SnsEventMessagePublisherTest extends AbstractSpringBootTest {

  private static final Duration CONSUMPTION_TIMEOUT = Duration.ofSeconds(60);

  @Autowired
  EventMessagePublisher publisher;

  @Autowired
  EventSerializer eventSerializer;

  @Autowired
  EventPayloadSerializer eventPayloadSerializer;

  @Autowired
  CalculatorRepository calculatorRepository;

  @Autowired
  AllTestFieldSavingEventHandler allTestFieldSavingEventHandler;

  @Test
  void shouldPublishEventConsumedByApplication() {
    var calculatorId = UUID.randomUUID().toString();

    publisher.publish(message(numberAdditionRequest(calculatorId, 5L)));

    awaitCalculatorNumber(calculatorId, 5L);
  }

  @Test
  void shouldPublishBatchLargerThanSnsLimitAcrossTopics() {
    var calculatorId = UUID.randomUUID().toString();
    var testFieldValue = "sns-batch " + UUID.randomUUID();

    var messages = new ArrayList<EventMessage>();
    IntStream.range(0, 23).forEach(i -> messages.add(message(numberAdditionRequest(calculatorId, 1L))));
    messages.add(message(testEntityEvent(testFieldValue + " #1")));
    messages.add(message(testEntityEvent(testFieldValue + " #2")));

    publisher.publish(messages);

    awaitCalculatorNumber(calculatorId, 23L);
    await()
        .atMost(CONSUMPTION_TIMEOUT)
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(() -> assertThat(allTestFieldSavingEventHandler.getSavedFields())
            .contains(testFieldValue + " #1", testFieldValue + " #2"));
  }

  @Test
  void shouldReportOnlyEventsOfMissingTopicAsFailed() {
    var calculatorId = UUID.randomUUID().toString();
    var withoutTopic = new EventMessage(
        new EventReference(new EntityReference("entity-without-topic", "1"), 1, "happened"),
        "{}");

    assertThatThrownBy(() -> publisher.publish(
        List.of(withoutTopic, message(numberAdditionRequest(calculatorId, 9L)))))
        .isInstanceOfSatisfying(EventPublicationException.class, exception ->
            assertThat(exception.failedEventReferences()).containsExactly(withoutTopic.eventReference()));

    awaitCalculatorNumber(calculatorId, 9L);
  }

  @Test
  void shouldPublishEventRelayedFromStoredParts() {
    var calculatorId = UUID.randomUUID().toString();
    var events = List.of(
        numberAdditionRequest(calculatorId, 3L),
        numberAdditionRequest(calculatorId, 4L));

    // Simulates a relay: builds messages from what an event store keeps, without the Event object
    var messages = events.stream()
        .map(event -> new EventMessage(
            event.eventReference(),
            eventSerializer.serialize(
                event.eventReference(),
                event.timestamp(),
                eventPayloadSerializer.serializePayload(event))))
        .toList();

    publisher.publish(messages);

    awaitCalculatorNumber(calculatorId, 7L);
  }

  private void awaitCalculatorNumber(String calculatorId, long expected) {
    await()
        .atMost(CONSUMPTION_TIMEOUT)
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(() -> assertThat(calculatorRepository.findBy(calculatorId))
            .hasValueSatisfying(calculator -> assertThat(calculator.getCurrentNumber()).isEqualTo(expected)));
  }

  private EventMessage message(Event<?> event) {
    return new EventMessage(event.eventReference(), eventSerializer.serialize(event));
  }

  private static Event<NumberAdditionRequest> numberAdditionRequest(String calculatorId, long number) {
    return Event.<NumberAdditionRequest>builder()
        .entityReference(new EntityReference(NUMBER_ADDITION_REQUEST, UUID.randomUUID().toString()))
        .eventName(RECEIVED)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(new NumberAdditionRequest(calculatorId, number))
        .build();
  }

  private static Event<TestEventPayload> testEntityEvent(String testFieldValue) {
    return Event.<TestEventPayload>builder()
        .entityReference(new EntityReference(TEST_ENTITY, UUID.randomUUID().toString()))
        .eventName(TEST_EVENT)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(new TestEventPayload(testFieldValue, "another"))
        .build();
  }
}
