package io.saga.caravan.event.consumer.messaging.sqs;

import io.saga.caravan.messaging.Message;
import io.saga.caravan.messaging.PollMessagesRequest;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SqsUtils {

  private static final String RECEIPT_HANDLE = "receiptHandle";

  private static final int MAX_MESSAGES_PER_POLL = 10;
  private static final int MAX_WAIT_TIME_SECONDS = 20;

  public static Collection<Message> pollMessagesFromQueue(SqsClient sqsClient,
                                                          String sqsQueueUrl,
                                                          PollMessagesRequest pollMessagesRequest) {
    List<Message> result = new ArrayList<>();

    int waitForSecondsLeft = pollMessagesRequest.waitForSeconds();
    int numberOfMessagesYetToPollInTotal = pollMessagesRequest.numberOfMessages();
    boolean fetchingSubsequentPages = false;

    while (waitForSecondsLeft > 0 && numberOfMessagesYetToPollInTotal > 0) {
      var maxNumberOfMessagesInIteration = Math.min(numberOfMessagesYetToPollInTotal, MAX_MESSAGES_PER_POLL);

      var waitTimeSecondsInIteration = fetchingSubsequentPages
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
        if (fetchingSubsequentPages) {
          return result;
        }

        waitForSecondsLeft -= waitTimeSecondsInIteration;
      } else {
        boolean receivedPartialPage = iterationPollResult.size() < maxNumberOfMessagesInIteration;
        if (receivedPartialPage) {
          return result;
        } else {
          fetchingSubsequentPages = true;
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

  public static void deleteMessage(SqsClient sqsClient, String queueUrl, Message message) {
    sqsClient.deleteMessage(
        DeleteMessageRequest.builder()
            .queueUrl(queueUrl)
            .receiptHandle(message.metadata().get(RECEIPT_HANDLE))
            .build());
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
      throw new IllegalStateException(
          "queueName=%s does not exist".formatted(queueName), exception);
    }
  }
}
