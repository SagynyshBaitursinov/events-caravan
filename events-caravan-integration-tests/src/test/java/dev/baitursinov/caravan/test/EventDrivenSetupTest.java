package dev.baitursinov.caravan.test;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.consumer.EventConsumer;
import dev.baitursinov.caravan.event.producer.DuplicateEventProductionException;
import dev.baitursinov.caravan.event.producer.EventProducer;
import dev.baitursinov.caravan.event.producer.EventProductionException;
import dev.baitursinov.caravan.test.event.handler.AllTestFieldSavingEventHandler;
import dev.baitursinov.caravan.test.event.handler.AnotherTestFieldSavingEventHandler;
import dev.baitursinov.caravan.test.event.handler.InterestingTestFieldSavingEventHandler;
import dev.baitursinov.caravan.test.event.handler.UnsureTestFieldSavingEventHandler;
import dev.baitursinov.caravan.test.value.AnotherTestEventPayloadRepresentation;
import dev.baitursinov.caravan.test.value.TestEventPayload;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.UUID;

import static dev.baitursinov.caravan.test.event.registration.TestEntityEventsConfiguration.ANOTHER_TEST_EVENT;
import static dev.baitursinov.caravan.test.event.registration.TestEntityEventsConfiguration.TEST_ENTITY;
import static dev.baitursinov.caravan.test.event.registration.TestEntityEventsConfiguration.TEST_EVENT;
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
        .atMost(Duration.ofSeconds(60))
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
  void shouldNotProduceEventWhosePayloadClassDiffersFromRegistered() {
    var testEventPayload = new AnotherTestEventPayloadRepresentation(
        "interesting test-value", "another-field-value");

    var event = Event.builder()
        .entityReference(
            new EntityReference(
                TEST_ENTITY, UUID.randomUUID().toString()))
        .eventName(TEST_EVENT)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(testEventPayload)
        .build();

    assertThatThrownBy(() -> eventProducer.produce(event))
        .isExactlyInstanceOf(EventProductionException.class)
        .hasMessageContaining(AnotherTestEventPayloadRepresentation.class.getName())
        .hasMessageContaining(TestEventPayload.class.getName());
  }

  @Test
  void shouldNotProduceEventOfUnregisteredType() {
    var event = Event.builder()
        .entityReference(
            new EntityReference(
                TEST_ENTITY, UUID.randomUUID().toString()))
        .eventName("unregistered-event")
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(new TestEventPayload("123", "456"))
        .build();

    assertThatThrownBy(() -> eventProducer.produce(event))
        .isExactlyInstanceOf(EventProductionException.class)
        .hasMessageContaining("unregistered-event");
  }

  @Test
  void differentEventsCanShareSamePayloadClass() {
    String testEventFieldValue = "shared-payload-class test-value";
    String anotherTestFieldValue = "shared-payload-class another-field-value";
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
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> {
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
