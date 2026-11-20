package dev.baitursinov.caravan.event.serialization;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.NoPayload;
import org.jspecify.annotations.Nullable;

/**
 * Serializes only an event's payload leaving its metadata
 * (entity reference, sequence number, event name, timestamp) to be handled separately by an
 * {@link EventSerializer}.
 * <p>
 * Implement this to control how payload classes are serialized.
 */
public interface EventPayloadSerializer {

  /**
   * @return the serialized payload, or {@code null} if the event's payload is {@link NoPayload},
   * in which case nothing is to be stored or sent for it
   */
  @Nullable String serializePayload(Event<?> event);
}
