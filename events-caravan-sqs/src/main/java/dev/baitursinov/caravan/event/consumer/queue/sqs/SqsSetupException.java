package dev.baitursinov.caravan.event.consumer.queue.sqs;

public class SqsSetupException extends RuntimeException {

  public SqsSetupException(String message, Exception cause) {
    super(message, cause);
  }
}
