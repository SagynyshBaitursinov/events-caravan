package dev.baitursinov.caravan.event.producer;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.EventReference;
import dev.baitursinov.caravan.event.publisher.EventMessage;
import dev.baitursinov.caravan.event.publisher.EventMessagePublicationException;
import dev.baitursinov.caravan.event.publisher.EventMessagePublisher;
import dev.baitursinov.caravan.event.serialization.EventSerializationException;
import dev.baitursinov.caravan.event.serialization.EventSerializer;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@NullMarked
class MessagePublishingUnsequencedEventWriterTest {

  static class RecordingEventMessagePublisher implements EventMessagePublisher {

    final List<EventMessage> publishedMessages = new ArrayList<>();
    final List<Integer> publishedBatchSizes = new ArrayList<>();
    @Nullable RuntimeException failure;

    @Override
    public void publish(EventMessage eventMessage) {
      publish(List.of(eventMessage));
    }

    @Override
    public void publish(List<EventMessage> eventMessages) {
      if (failure != null) {
        throw failure;
      }
      publishedBatchSizes.add(eventMessages.size());
      publishedMessages.addAll(eventMessages);
    }
  }

  static class BodyNamingEventSerializer implements EventSerializer {

    @Override
    public String serialize(Event<?> event) {
      if (event.payload().equals("unserializable")) {
        throw new EventSerializationException("Could not serialize " + event.eventReference());
      }
      return "body of " + event.entityReference().entityId();
    }

    @Override
    public String serialize(EventReference eventReference, ZonedDateTime timestamp, String serializedPayload) {
      throw new UnsupportedOperationException();
    }
  }

  RecordingEventMessagePublisher publisher = new RecordingEventMessagePublisher();

  MessagePublishingUnsequencedEventWriter writer =
      new MessagePublishingUnsequencedEventWriter(new BodyNamingEventSerializer(), publisher);

  @Test
  void shouldPublishSerializedEventsAsOneBatch() {
    var first = event("1", "payload");
    var second = event("2", "payload");

    writer.write(validated(first, second));

    assertThat(publisher.publishedMessages).containsExactly(
        new EventMessage(first.eventReference(), "body of 1"),
        new EventMessage(second.eventReference(), "body of 2"));
    assertThat(publisher.publishedBatchSizes).containsExactly(2);
  }

  @Test
  void shouldPublishSameEventAgainWithoutDetectingDuplicate() {
    var event = event("1", "payload");

    writer.write(validated(event));
    writer.write(validated(event));

    assertThat(publisher.publishedMessages).hasSize(2);
  }

  @Test
  void shouldPublishNothingForEmptyList() {
    writer.write(validated());

    assertThat(publisher.publishedBatchSizes).isEmpty();
  }

  @Test
  void shouldFailProductionWhenPublicationFails() {
    var event = event("1", "payload");
    var publicationException = new EventMessagePublicationException(List.of(event.eventReference()), "broker is down");
    publisher.failure = publicationException;

    assertThatThrownBy(() -> writer.write(validated(event)))
        .isInstanceOf(EventProductionException.class)
        .hasCause(publicationException);
  }

  @Test
  void shouldFailProductionWhenPublisherFailsUnexpectedly() {
    publisher.failure = new IllegalStateException("boom");

    assertThatThrownBy(() -> writer.write(validated(event("1", "payload"))))
        .isInstanceOf(EventProductionException.class)
        .hasCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  void shouldPublishNothingWhenAnyEventCannotBeSerialized() {
    assertThatThrownBy(() -> writer.write(validated(event("1", "payload"), event("2", "unserializable"))))
        .isInstanceOf(EventProductionException.class)
        .hasCauseInstanceOf(EventSerializationException.class);

    assertThat(publisher.publishedMessages).isEmpty();
  }

  private static ValidatedEvents validated(Event<?>... events) {
    return new ValidatedEvents(List.of(events));
  }

  private static Event<?> event(String entityId, String payload) {
    return Event.unsequenced()
        .entityReference(new EntityReference("payment", entityId))
        .eventName("received")
        .naturalKey("event-of-" + entityId)
        .timestamp(ZonedDateTime.now())
        .payload(payload)
        .build();
  }
}
