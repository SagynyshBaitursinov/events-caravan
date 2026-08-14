package dev.baitursinov.caravan.event.sourcing.snapshot;

public class SnapshotException extends RuntimeException {

  public SnapshotException(String message) {
    super(message);
  }
}
