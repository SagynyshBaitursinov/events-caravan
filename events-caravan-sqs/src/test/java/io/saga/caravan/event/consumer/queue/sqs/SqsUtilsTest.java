package io.saga.caravan.event.consumer.queue.sqs;

import io.saga.caravan.queue.polling.Message;
import io.saga.caravan.queue.polling.PollMessagesRequest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static io.saga.caravan.event.consumer.queue.sqs.SqsUtils.deleteMessages;
import static io.saga.caravan.event.consumer.queue.sqs.SqsUtils.getQueueUrl;
import static io.saga.caravan.event.consumer.queue.sqs.SqsUtils.pollMessagesFromQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SqsUtilsTest {

  private static software.amazon.awssdk.services.sqs.model.Message sqsMessage(String id, String body, String receiptHandle) {
    return software.amazon.awssdk.services.sqs.model.Message.builder()
        .messageId(id)
        .body(body)
        .receiptHandle(receiptHandle)
        .build();
  }

  private static ReceiveMessageResponse responseWith(software.amazon.awssdk.services.sqs.model.Message... messages) {
    return ReceiveMessageResponse.builder().messages(List.of(messages)).build();
  }

  @Nested
  class SinglePagePolling {

    @Test
    void mapsSqsMessagesToDomainMessagesIncludingReceiptHandleMetadata() {
      SqsClient sqsClient = mock(SqsClient.class);
      when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
          .thenReturn(responseWith(
              sqsMessage("id-1", "body-1", "receipt-1")));

      var result = pollMessagesFromQueue(sqsClient, "queue-url", 5, 10);

      assertThat(result).hasSize(1);
      Message message = result.iterator().next();
      assertThat(message.id()).isEqualTo("id-1");
      assertThat(message.body()).isEqualTo("body-1");
      assertThat(message.metadata()).containsEntry("receiptHandle", "receipt-1");
    }

    @Test
    void sendsTheRequestedMaxNumberOfMessagesAndWaitTime() {
      SqsClient sqsClient = mock(SqsClient.class);
      when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
          .thenReturn(responseWith());

      pollMessagesFromQueue(sqsClient, "queue-url", 7, 12);

      ArgumentCaptor<ReceiveMessageRequest> captor = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
      verify(sqsClient).receiveMessage(captor.capture());
      assertThat(captor.getValue().queueUrl()).isEqualTo("queue-url");
      assertThat(captor.getValue().maxNumberOfMessages()).isEqualTo(7);
      assertThat(captor.getValue().waitTimeSeconds()).isEqualTo(12);
    }

    @Test
    void returnsAnEmptyCollectionWhenNoMessagesAreAvailable() {
      SqsClient sqsClient = mock(SqsClient.class);
      when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
          .thenReturn(responseWith());

      var result = pollMessagesFromQueue(sqsClient, "queue-url", 5, 10);

      assertThat(result).isEmpty();
    }
  }

  @Nested
  class PaginatedPolling {

    @Test
    void stopsAfterSinglePartialPageEvenWithWaitTimeAndCountRemaining() {
      SqsClient sqsClient = mock(SqsClient.class);
      when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
          .thenReturn(responseWith(
              sqsMessage("1", "b1", "r1"),
              sqsMessage("2", "b2", "r2")));

      var result = pollMessagesFromQueue(
          sqsClient, "queue-url",
          PollMessagesRequest.builder()
              .numberOfMessages(10)
              .waitForSeconds(15)
              .build());

      assertThat(result).hasSize(2);
      verify(sqsClient, times(1)).receiveMessage(any(ReceiveMessageRequest.class));
    }

    @Test
    void keepsFetchingFullPagesWithoutWaitingUntilTheRequestedCountIsReached() {
      SqsClient sqsClient = mock(SqsClient.class);
      var firstPage =
          IntStream.range(0, 10).mapToObj(i -> sqsMessage("p1-" + i, "b" + i, "r" + i)).toList();
      var secondPage =
          IntStream.range(0, 5).mapToObj(i -> sqsMessage("p2-" + i, "b" + i, "r" + i)).toList();

      when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
          .thenReturn(ReceiveMessageResponse.builder().messages(firstPage).build())
          .thenReturn(ReceiveMessageResponse.builder().messages(secondPage).build());

      var result = pollMessagesFromQueue(
          sqsClient, "queue-url",
          PollMessagesRequest.builder().numberOfMessages(15).waitForSeconds(30).build());

      assertThat(result).hasSize(15);

      ArgumentCaptor<ReceiveMessageRequest> captor = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
      verify(sqsClient, times(2)).receiveMessage(captor.capture());
      List<ReceiveMessageRequest> requests = captor.getAllValues();
      assertThat(requests.get(0).maxNumberOfMessages()).isEqualTo(10);
      assertThat(requests.get(0).waitTimeSeconds()).isEqualTo(20);
      assertThat(requests.get(1).maxNumberOfMessages()).isEqualTo(5);
      assertThat(requests.get(1).waitTimeSeconds()).isEqualTo(0);
    }

    @Test
    void retriesOnEmptyPagesConsumingTheWaitBudgetThenReturnsPartialPage() {
      SqsClient sqsClient = mock(SqsClient.class);
      when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
          .thenReturn(responseWith())
          .thenReturn(responseWith())
          .thenReturn(responseWith(
              sqsMessage("1", "b1", "r1"),
              sqsMessage("2", "b2", "r2")));

      var result = pollMessagesFromQueue(
          sqsClient, "queue-url",
          PollMessagesRequest.builder().numberOfMessages(5).waitForSeconds(45).build());

      assertThat(result).hasSize(2);

      ArgumentCaptor<ReceiveMessageRequest> captor = ArgumentCaptor.forClass(ReceiveMessageRequest.class);
      verify(sqsClient, times(3)).receiveMessage(captor.capture());
      assertThat(captor.getAllValues())
          .extracting(ReceiveMessageRequest::waitTimeSeconds)
          .containsExactly(20, 20, 5);
    }

    @Test
    void doesNotPollAtAllWhenNoWaitTimeIsRequested() {
      SqsClient sqsClient = mock(SqsClient.class);

      var result = pollMessagesFromQueue(
          sqsClient, "queue-url",
          PollMessagesRequest.builder().numberOfMessages(5).waitForSeconds(0).build());

      assertThat(result).isEmpty();
      verify(sqsClient, never()).receiveMessage(any(ReceiveMessageRequest.class));
    }
  }

  @Nested
  class Deletion {

    @Test
    void deletesUpTo10MessagesInASingleBatchRequest() {
      SqsClient sqsClient = mock(SqsClient.class);
      when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
          .thenReturn(responseWith(
              sqsMessage("1", "b1", "r1"),
              sqsMessage("2", "b2", "r2")));
      List<Message> messages = new ArrayList<>(
          pollMessagesFromQueue(sqsClient, "queue-url", 2, 5));

      when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
          .thenReturn(DeleteMessageBatchResponse.builder().failed(List.of()).build());

      deleteMessages(sqsClient, "queue-url", messages);

      ArgumentCaptor<DeleteMessageBatchRequest> captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
      verify(sqsClient, times(1)).deleteMessageBatch(captor.capture());
      DeleteMessageBatchRequest request = captor.getValue();
      assertThat(request.queueUrl()).isEqualTo("queue-url");
      assertThat(request.entries()).hasSize(2);
      assertThat(request.entries().get(0).receiptHandle()).isEqualTo("r1");
      assertThat(request.entries().get(1).receiptHandle()).isEqualTo("r2");
    }

    @Test
    void splitsMoreThan10MessagesAcrossMultipleDeleteBatches() {
      SqsClient sqsClient = mock(SqsClient.class);
      var firstPage =
          IntStream.range(0, 10).mapToObj(i -> sqsMessage("p1-" + i, "b" + i, "r1-" + i)).toList();
      var secondPage =
          IntStream.range(0, 5).mapToObj(i -> sqsMessage("p2-" + i, "b" + i, "r2-" + i)).toList();
      when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
          .thenReturn(ReceiveMessageResponse.builder().messages(firstPage).build())
          .thenReturn(ReceiveMessageResponse.builder().messages(secondPage).build());

      List<Message> messages = new ArrayList<>();
      messages.addAll(pollMessagesFromQueue(sqsClient, "queue-url", 10, 5));
      messages.addAll(pollMessagesFromQueue(sqsClient, "queue-url", 5, 5));
      assertThat(messages).hasSize(15);

      when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
          .thenReturn(DeleteMessageBatchResponse.builder().failed(List.of()).build());

      deleteMessages(sqsClient, "queue-url", messages);

      ArgumentCaptor<DeleteMessageBatchRequest> captor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
      verify(sqsClient, times(2)).deleteMessageBatch(captor.capture());
      List<DeleteMessageBatchRequest> requests = captor.getAllValues();
      assertThat(requests.get(0).entries()).hasSize(10);
      assertThat(requests.get(1).entries()).hasSize(5);
      assertThat(requests.get(1).entries().getFirst().receiptHandle()).isEqualTo("r2-0");
    }

    @Test
    void doesNotCallDeleteMessageBatchForAnEmptyList() {
      SqsClient sqsClient = mock(SqsClient.class);

      deleteMessages(sqsClient, "queue-url", List.of());

      verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void doesNotThrowWhenTheBatchResponseReportsFailures() {
      SqsClient sqsClient = mock(SqsClient.class);
      when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
          .thenReturn(responseWith(sqsMessage("1", "b1", "r1")));
      List<Message> messages = new ArrayList<>(pollMessagesFromQueue(sqsClient, "queue-url", 1, 5));

      when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
          .thenReturn(DeleteMessageBatchResponse.builder()
              .failed(BatchResultErrorEntry.builder().id("0").code("InternalError").message("boom").build())
              .build());

      assertThatCode(() -> deleteMessages(sqsClient, "queue-url", messages))
          .doesNotThrowAnyException();
    }
  }

  @Nested
  class QueueUrlLookup {

    @Test
    void returnsTheQueueUrlFromSqs() {
      SqsClient sqsClient = mock(SqsClient.class);
      when(sqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
          .thenReturn(GetQueueUrlResponse.builder().queueUrl("https://sqs.example.com/queue").build());

      String queueUrl = getQueueUrl(sqsClient, "my-queue");

      assertThat(queueUrl).isEqualTo("https://sqs.example.com/queue");
      ArgumentCaptor<GetQueueUrlRequest> captor = ArgumentCaptor.forClass(GetQueueUrlRequest.class);
      verify(sqsClient).getQueueUrl(captor.capture());
      assertThat(captor.getValue().queueName()).isEqualTo("my-queue");
    }

    @SuppressWarnings("resource")
    @Test
    void wrapsQueueDoesNotExistExceptionInAnSqsException() {
      SqsClient sqsClient = mock(SqsClient.class);
      QueueDoesNotExistException original = QueueDoesNotExistException.builder().message("no such queue").build();
      when(sqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenThrow(original);

      assertThatThrownBy(() -> getQueueUrl(sqsClient, "missing-queue"))
          .isInstanceOf(SqsSetupException.class)
          .hasMessage("queueName=missing-queue does not exist")
          .hasCause(original);
    }
  }
}
