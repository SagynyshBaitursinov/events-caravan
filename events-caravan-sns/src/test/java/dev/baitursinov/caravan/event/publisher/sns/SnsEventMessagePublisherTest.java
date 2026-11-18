package dev.baitursinov.caravan.event.publisher.sns;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.EventReference;
import dev.baitursinov.caravan.event.SequencedEventReference;
import dev.baitursinov.caravan.event.publisher.EventMessage;
import dev.baitursinov.caravan.event.publisher.EventMessagePublicationException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.BatchRequestTooLongException;
import software.amazon.awssdk.services.sns.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sns.model.NotFoundException;
import software.amazon.awssdk.services.sns.model.PublishBatchRequest;
import software.amazon.awssdk.services.sns.model.PublishBatchResponse;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SnsEventMessagePublisherTest {

  SnsClient snsClient = mock(SnsClient.class);

  SnsTopicArnResolver topicArnResolver =
      SnsTopicArnResolver.byExpectation("aws", "us-east-1", "123456789012", "events");

  SnsEventMessagePublisher publisher = new SnsEventMessagePublisher(snsClient, topicArnResolver);

  @Test
  void shouldResolveTopicArnLikeTheLambda() {
    assertThat(topicArnResolver.topicArnFor("car"))
        .isEqualTo("arn:aws:sns:us-east-1:123456789012:events_car");
  }

  @Test
  void shouldPublishSingleMessageWithAttributes() {
    when(snsClient.publish(any(PublishRequest.class))).thenReturn(PublishResponse.builder().build());

    publisher.publish(message("car", 1));

    var captor = ArgumentCaptor.forClass(PublishRequest.class);
    verify(snsClient).publish(captor.capture());
    var request = captor.getValue();
    assertThat(request.topicArn()).isEqualTo("arn:aws:sns:us-east-1:123456789012:events_car");
    assertThat(request.message()).isEqualTo("body-car-1");
    assertThat(request.messageAttributes().get("entityName").stringValue()).isEqualTo("car");
    assertThat(request.messageAttributes().get("entityName").dataType()).isEqualTo("String");
    assertThat(request.messageAttributes().get("eventName").stringValue()).isEqualTo("turned-on");
  }

  @Test
  void shouldGroupByTopicAndSplitIntoBatchesOfTen() {
    when(snsClient.publishBatch(any(PublishBatchRequest.class)))
        .thenReturn(PublishBatchResponse.builder().build());

    var messages = new java.util.ArrayList<>(IntStream.rangeClosed(1, 11).mapToObj(i -> message("car", i)).toList());
    messages.add(message("boat", 1));
    messages.add(message("boat", 2));

    publisher.publish(messages);

    var captor = ArgumentCaptor.forClass(PublishBatchRequest.class);
    verify(snsClient, times(2)).publishBatch(captor.capture());
    verify(snsClient).publish(any(PublishRequest.class));

    assertThat(captor.getAllValues())
        .extracting(PublishBatchRequest::topicArn, request -> request.publishBatchRequestEntries().size())
        .containsExactly(
            tuple("arn:aws:sns:us-east-1:123456789012:events_car", 10),
            tuple("arn:aws:sns:us-east-1:123456789012:events_boat", 2));
  }

  @Test
  void shouldReportPartiallyFailedEntries() {
    when(snsClient.publishBatch(any(PublishBatchRequest.class)))
        .thenReturn(PublishBatchResponse.builder()
            .failed(BatchResultErrorEntry.builder().id("1").code("InternalError").message("oops").build())
            .build());

    assertThatThrownBy(() -> publisher.publish(List.of(message("car", 1), message("car", 2), message("car", 3))))
        .isInstanceOfSatisfying(EventMessagePublicationException.class, exception ->
            assertThat(exception.failedEventReferences()).containsExactly(reference("car", 2)));
  }

  @Test
  void shouldFallBackToSinglePublishesWhenBatchIsTooLong() {
    when(snsClient.publishBatch(any(PublishBatchRequest.class)))
        .thenThrow(BatchRequestTooLongException.builder().message("too long").build());
    when(snsClient.publish(any(PublishRequest.class)))
        .thenReturn(PublishResponse.builder().build())
        .thenThrow(NotFoundException.builder().message("gone").build());

    assertThatThrownBy(() -> publisher.publish(List.of(message("car", 1), message("car", 2))))
        .isInstanceOfSatisfying(EventMessagePublicationException.class, exception ->
            assertThat(exception.failedEventReferences()).containsExactly(reference("car", 2)));

    verify(snsClient, times(2)).publish(any(PublishRequest.class));
  }

  @Test
  void shouldContinueWithOtherTopicsWhenOneTopicFails() {
    when(snsClient.publish(any(PublishRequest.class)))
        .thenThrow(NotFoundException.builder().message("no topic").build())
        .thenReturn(PublishResponse.builder().build());

    assertThatThrownBy(() -> publisher.publish(List.of(message("car", 1), message("boat", 1))))
        .isInstanceOfSatisfying(EventMessagePublicationException.class, exception ->
            assertThat(exception.failedEventReferences()).containsExactly(reference("car", 1)))
        .hasCauseInstanceOf(NotFoundException.class);

    verify(snsClient, times(2)).publish(any(PublishRequest.class));
    verify(snsClient, never()).publishBatch(any(PublishBatchRequest.class));
  }

  private static EventMessage message(String entityName, long sequenceNumber) {
    return new EventMessage(reference(entityName, sequenceNumber), "body-%s-%d".formatted(entityName, sequenceNumber));
  }

  private static EventReference reference(String entityName, long sequenceNumber) {
    return new SequencedEventReference(new EntityReference(entityName, "1"), sequenceNumber, "turned-on");
  }
}
