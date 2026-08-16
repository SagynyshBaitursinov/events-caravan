package dev.baitursinov.caravan.event.sourcing.entity.stream;

public class EntityStreamRegistrationException extends RuntimeException {

  public EntityStreamRegistrationException(String message) {
    super(message);
  }
}
