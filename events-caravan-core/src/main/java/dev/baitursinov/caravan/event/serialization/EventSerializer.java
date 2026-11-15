package dev.baitursinov.caravan.event.serialization;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.EventReference;

import java.time.ZonedDateTime;

/**
 * Serializes a whole {@link Event}, including its metadata.
 * <p>
 * Implement this to use a different format.
 */
public interface EventSerializer {

  String serialize(Event<?> event) throws EventSerializationException;

  /**
   * Serializes a whole event from its metadata and an already serialized payload, as an event
   * store might keep separately.
   *
   * <p>The result must be identical to {@link #serialize(Event)} of the same event.
   *
   * @param eventReference    entity reference, sequence number and event name of the event
   * @param timestamp         when the event happened; normalized like {@link Event#timestamp()}
   * @param serializedPayload the event's payload, as produced by an {@link EventPayloadSerializer}
   * @throws EventSerializationException if the event cannot be serialized
   */
  String serialize(EventReference eventReference,
                   ZonedDateTime timestamp,
                   String serializedPayload) throws EventSerializationException;
}
