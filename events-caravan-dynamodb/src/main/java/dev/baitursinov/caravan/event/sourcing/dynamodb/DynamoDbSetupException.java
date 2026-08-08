package dev.baitursinov.caravan.event.sourcing.dynamodb;

public class DynamoDbSetupException extends RuntimeException {

  public DynamoDbSetupException(String message) {
    super(message);
  }

  public DynamoDbSetupException(String message, Exception cause) {
    super(message, cause);
  }
}
