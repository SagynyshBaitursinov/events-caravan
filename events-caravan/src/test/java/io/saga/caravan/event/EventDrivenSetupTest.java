package io.saga.caravan.event;

import io.saga.caravan.AbstractSpringBootTest;
import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.consumer.EventConsumer;
import io.saga.caravan.event.producer.DuplicateEventProductionException;
import io.saga.caravan.event.producer.EventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.UUID;

import static io.saga.caravan.event.TestEntityEventsConfiguration.ANOTHER_TEST_EVENT;
import static io.saga.caravan.event.TestEntityEventsConfiguration.TEST_ENTITY;
import static io.saga.caravan.event.TestEntityEventsConfiguration.TEST_EVENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class EventDrivenSetupTest extends AbstractSpringBootTest {

  @Autowired
  EventProducer eventProducer;

  @Autowired
  EventConsumer eventConsumer;

  @Autowired
  InterestingTestFieldSavingEventHandler interestingTestFieldSavingEventHandler;

  @Autowired
  AllTestFieldSavingEventHandler allTestFieldSavingEventHandler;

  @Autowired
  AnotherTestFieldSavingEventHandler anotherTestFieldSavingEventHandler;

  @Autowired
  UnsureTestFieldSavingEventHandler<?> unsureTestFieldSavingEventHandler;

  @Test
  void cannotProduceEventsOfSameEntityWithSameSequenceNumber() {
    var testEventPayload = new TestEventPayload("123", "456");
    String entityId = UUID.randomUUID().toString();

    var event1 = Event.builder()
        .entityReference(
            new EntityReference(TEST_ENTITY, entityId))
        .eventName(TEST_EVENT)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(testEventPayload)
        .build();
    eventProducer.produce(event1);

    var event2 = Event.builder()
        .entityReference(
            new EntityReference(TEST_ENTITY, entityId))
        .eventName(TEST_EVENT)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(testEventPayload)
        .build();
    assertThatThrownBy(() -> eventProducer.produce(event2))
        .isExactlyInstanceOf(DuplicateEventProductionException.class);
  }

  @Test
  void shouldProduceAndConsumeEventByAllHandlers() {
    String testEventFieldValue = "interesting test-value";
    String anotherTestFieldValue = "another-field-value";
    var testEventPayload = new TestEventPayload(
        testEventFieldValue, anotherTestFieldValue);

    var event = Event.builder()
        .entityReference(
            new EntityReference(
                TEST_ENTITY, UUID.randomUUID().toString()))
        .eventName(TEST_EVENT)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(testEventPayload)
        .build();

    eventProducer.produce(event);

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> {
          assertThat(interestingTestFieldSavingEventHandler.getSavedFields())
              .contains(testEventFieldValue);
          assertThat(allTestFieldSavingEventHandler.getSavedFields())
              .contains(testEventFieldValue);
          assertThat(anotherTestFieldSavingEventHandler.getSavedFields())
              .contains(anotherTestFieldValue);
        });
  }

  @Test
  void shouldNotConsumeNotInterestingEvent() {
    String testEventFieldValue = "not-interesting test-value";
    String anotherTestFieldValue = "another-field-value";
    var testEventPayload = new TestEventPayload(
        testEventFieldValue,
        anotherTestFieldValue);

    var event = Event.builder()
        .entityReference(
            new EntityReference(
                TEST_ENTITY, UUID.randomUUID().toString()))
        .eventName(TEST_EVENT)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(testEventPayload)
        .build();

    eventConsumer.consume(event);

    assertThat(interestingTestFieldSavingEventHandler.getSavedFields())
        .doesNotContain(testEventFieldValue);
    assertThat(allTestFieldSavingEventHandler.getSavedFields())
        .contains(testEventFieldValue);
    assertThat(anotherTestFieldSavingEventHandler.getSavedFields())
        .contains(anotherTestFieldValue);
  }

  @Test
  void shouldConsumeTestEventEvenIfItWasProducedAsAnotherClass() {
    String testEventFieldValue = "interesting test-value";
    String anotherTestFieldValue = "another-field-value";
    var testEventPayload = new AnotherTestEventPayloadRepresentation(
        testEventFieldValue, anotherTestFieldValue);

    var event = Event.builder()
        .entityReference(
            new EntityReference(
                TEST_ENTITY, UUID.randomUUID().toString()))
        .eventName(TEST_EVENT)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(testEventPayload)
        .build();

    eventProducer.produce(event);

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> {
          assertThat(interestingTestFieldSavingEventHandler.getSavedFields())
              .contains(testEventFieldValue);
          assertThat(allTestFieldSavingEventHandler.getSavedFields())
              .contains(testEventFieldValue);
          assertThat(anotherTestFieldSavingEventHandler.getSavedFields())
              .contains(anotherTestFieldValue);
        });
  }

  @Test
  void differentEventsCanShareSamePayloadClass() {
    String testEventFieldValue = "interesting test-value";
    String anotherTestFieldValue = "another-field-value";
    var testEventPayload = new TestEventPayload(
        testEventFieldValue, anotherTestFieldValue);

    var event = Event.builder()
        .entityReference(
            new EntityReference(
                TEST_ENTITY, UUID.randomUUID().toString()))
        .eventName(ANOTHER_TEST_EVENT)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(testEventPayload)
        .build();

    eventProducer.produce(event);

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> {
          assertThat(interestingTestFieldSavingEventHandler.getSavedFields())
              .contains(testEventFieldValue);
          assertThat(allTestFieldSavingEventHandler.getSavedFields())
              .contains(testEventFieldValue);
          assertThat(anotherTestFieldSavingEventHandler.getSavedFields())
              .contains(anotherTestFieldValue);
        });
  }

  @Test
  void shouldNotBeConsumedByUnsureHandler() {
    String testEventFieldValue = "not-interesting test-value";
    String anotherTestFieldValue = "another-field-value";
    var testEventPayload = new TestEventPayload(
        testEventFieldValue,
        anotherTestFieldValue);

    var event = Event.builder()
        .entityReference(
            new EntityReference(
                TEST_ENTITY, UUID.randomUUID().toString()))
        .eventName(TEST_EVENT)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(testEventPayload)
        .build();

    eventConsumer.consume(event);

    assertThat(unsureTestFieldSavingEventHandler.getSavedFields()).isEmpty();
  }
}
