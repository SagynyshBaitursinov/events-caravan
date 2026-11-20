package dev.baitursinov.caravan.event;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Payload of events that carry no details besides their metadata, for which the event name alone
 * says everything that happened.
 *
 * <p>Register it as the payload class of such events in an {@link EntityEventsRegistration}, and
 * use {@link #INSTANCE} as their payload. Its only purpose is to keep {@link Event#payload()}
 * non-null: nothing is stored or sent for it, and it is restored as {@link #INSTANCE} when the
 * event is read back.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NoPayload {

  public static final NoPayload INSTANCE = new NoPayload();

  @Override
  public String toString() {
    return "NoPayload";
  }
}
