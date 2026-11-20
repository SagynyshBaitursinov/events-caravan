package dev.baitursinov.caravan.event.serialization;

import dev.baitursinov.caravan.event.EventType;
import dev.baitursinov.caravan.event.NoPayload;
import org.jspecify.annotations.Nullable;

/**
 * Deserializes only an event's payload from the wire format produced by a matching
 * {@link EventPayloadSerializer}, resolving the target payload class from the event's
 * {@link EventType}.
 */
public interface EventPayloadDeserializer {

  /**
   * @param payload the serialized payload, or {@code null} if the event carries none
   * @return the payload; {@link NoPayload#INSTANCE} for an event whose payload class is
   * {@link NoPayload}
   * @throws EventPayloadDeserializationException if the payload cannot be deserialized, is
   *                                              missing for a payload class other than
   *                                              {@link NoPayload}, or present for it
   */
  Object deserializePayload(@Nullable String payload,
                            EventType eventType) throws EventPayloadDeserializationException;
}
