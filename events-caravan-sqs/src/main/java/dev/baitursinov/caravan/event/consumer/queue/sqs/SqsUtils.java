package dev.baitursinov.caravan.event.consumer.queue.sqs;

import dev.baitursinov.caravan.queue.polling.Message;
import dev.baitursinov.caravan.queue.polling.PollMessagesRequest;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SqsUtils {

  private static final String RECEIPT_HANDLE = "receiptHandle";

  private static final int MAX_MESSAGES_PER_POLL = 10;
  private static final int MAX_WAIT_TIME_SECONDS = 20;
  private static final int MAX_MESSAGES_PER_DELETE_BATCH = 10;

  public static Collection<Message> pollMessagesFromQueue(SqsClient sqsClient,
                                                          String sqsQueueUrl,
                                                          PollMessagesRequest pollMessagesRequest) {
    List<Message> result = new ArrayList<>();

    int waitForSecondsLeft = pollMessagesRequest.waitForSeconds();
    int numberOfMessagesYetToPollInTotal = pollMessagesRequest.numberOfMessages();
    boolean isFetchingSubsequentPages = false;

    while (waitForSecondsLeft > 0 && numberOfMessagesYetToPollInTotal > 0) {
      var maxNumberOfMessagesInIteration = Math.min(numberOfMessagesYetToPollInTotal, MAX_MESSAGES_PER_POLL);

      var waitTimeSecondsInIteration = isFetchingSubsequentPages
          ? 0
          : Math.min(waitForSecondsLeft, MAX_WAIT_TIME_SECONDS);

      var iterationPollResult = pollMessagesFromQueue(
          sqsClient,
          sqsQueueUrl,
          maxNumberOfMessagesInIteration,
          waitTimeSecondsInIteration);

      result.addAll(iterationPollResult);
      numberOfMessagesYetToPollInTotal -= iterationPollResult.size();

      if (iterationPollResult.isEmpty()) {
        if (isFetchingSubsequentPages) {
          return result;
        }

        waitForSecondsLeft -= waitTimeSecondsInIteration;
      } else {
        boolean hasReceivedPartialPage = iterationPollResult.size() < maxNumberOfMessagesInIteration;
        if (hasReceivedPartialPage) {
          return result;
        } else {
          isFetchingSubsequentPages = true;
        }
      }
    }

    return result;
  }

  public static Collection<Message> pollMessagesFromQueue(SqsClient sqsClient,
                                                          String sqsQueueUrl,
                                                          int maxNumberOfMessages,
                                                          int waitTimeSeconds) {
    return sqsClient
        .receiveMessage(
            ReceiveMessageRequest.builder()
                .queueUrl(sqsQueueUrl)
                .maxNumberOfMessages(maxNumberOfMessages)
                .waitTimeSeconds(waitTimeSeconds)
                .build())
        .messages()
        .stream()
        .map(message -> new Message(
            message.messageId(),
            message.body(),
            Map.of(RECEIPT_HANDLE, message.receiptHandle())))
        .toList();
  }

  public static void deleteMessages(SqsClient sqsClient, String queueUrl, List<Message> messages) {
    for (int from = 0; from < messages.size(); from += MAX_MESSAGES_PER_DELETE_BATCH) {
      deleteMessagesBatch(
          sqsClient,
          queueUrl,
          messages.subList(from, Math.min(from + MAX_MESSAGES_PER_DELETE_BATCH, messages.size())));
    }
  }

  private static void deleteMessagesBatch(SqsClient sqsClient, String queueUrl, List<Message> batch) {
    var entries = IntStream.range(0, batch.size())
        .mapToObj(index ->
            DeleteMessageBatchRequestEntry.builder()
                .id(String.valueOf(index))
                .receiptHandle(batch.get(index).metadata().get(RECEIPT_HANDLE))
                .build())
        .toList();

    var response = sqsClient.deleteMessageBatch(
        DeleteMessageBatchRequest.builder()
            .queueUrl(queueUrl)
            .entries(entries)
            .build());

    response.failed().forEach(failure ->
        log.warn(
            "Failed to delete messageId={}: code={}, message={}",
            batch.get(Integer.parseInt(failure.id())).id(), failure.code(), failure.message()));
  }

  public static String getQueueUrl(SqsClient sqsClient,
                                   String queueName) {
    try {
      return sqsClient
          .getQueueUrl(
              GetQueueUrlRequest.builder()
                  .queueName(queueName)
                  .build())
          .queueUrl();
    } catch (QueueDoesNotExistException exception) {
      throw new SqsSetupException(
          "queueName=%s does not exist".formatted(queueName), exception);
    }
  }
}
