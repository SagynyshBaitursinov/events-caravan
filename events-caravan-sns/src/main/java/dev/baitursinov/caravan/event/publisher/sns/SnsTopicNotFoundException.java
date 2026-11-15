package dev.baitursinov.caravan.event.publisher.sns;

public class SnsTopicNotFoundException extends RuntimeException {

  public SnsTopicNotFoundException(String message) {
    super(message);
  }
}
