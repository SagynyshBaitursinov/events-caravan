package dev.baitursinov.caravan.event.sourcing.entity.stream;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.consumer.handler.EventHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Write every entity into the stream upon its first event.
 */
@Slf4j
@RequiredArgsConstructor
public final class EntityStreamWritingEventHandler implements EventHandler<Object> {

  private static final long FIRST_SEQUENCE_NUMBER = 1;

  private final EntityStreamWriter entityStreamWriter;

  @Override
  public boolean isOfInterest(Event<Object> event) {
    return event.sequenceNumber() == FIRST_SEQUENCE_NUMBER;
  }

  @Override
  public void handle(Event<Object> event) {
    entityStreamWriter.write(
        new EntityStreamEntry(event.entityReference(), event.timestamp()));
    log.debug("Wrote {} to stream", event.entityReference());
  }
}
