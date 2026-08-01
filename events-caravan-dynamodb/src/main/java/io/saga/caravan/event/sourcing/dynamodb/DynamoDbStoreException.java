package io.saga.caravan.event.sourcing.dynamodb;

public class DynamoDbStoreException extends RuntimeException {

  public DynamoDbStoreException(String message) {
    super(message);
  }
}
