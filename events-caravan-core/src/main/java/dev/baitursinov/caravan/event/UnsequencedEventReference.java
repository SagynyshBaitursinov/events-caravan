package dev.baitursinov.caravan.event;

import dev.baitursinov.caravan.entity.EntityReference;

import static dev.baitursinov.caravan.utils.TextUtils.hasText;
import static java.util.Objects.requireNonNull;

/**
 * Identifies an event that is not a member of its entity's sequence, named {@code eventName} and
 * told apart from the entity's other events by {@code naturalKey}.
 *
 * <p>{@code naturalKey} is given by whoever produces the event, derived from the business fact the
 * event describes (e.g. a session id or an incoming request id) rather than generated, and only has
 * to be unique among its entity's events of the same name. It has to stay the same when producing the same event again, e.g. on a retry,
 * so that consumers can recognize the duplicate. The framework does not deduplicate events by it:
 * a repeated production is delivered again, and skipping it is up to consumers.
 */
public record UnsequencedEventReference(EntityReference entityReference,
                                        String naturalKey,
                                        String eventName) implements EventReference {

  public UnsequencedEventReference {
    requireNonNull(entityReference, "entityReference cannot be null");

    if (!hasText(naturalKey)) {
      throw new IllegalArgumentException("naturalKey cannot be empty");
    }

    if (!hasText(eventName)) {
      throw new IllegalArgumentException("eventName cannot be empty");
    }
  }

  @Override
  public String toString() {
    return "Event{"
        + this.entityReference()
        + ":" + this.naturalKey()
        + "(" + this.eventName() + ")}";
  }
}
