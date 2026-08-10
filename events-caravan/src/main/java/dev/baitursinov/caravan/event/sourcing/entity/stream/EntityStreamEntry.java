package dev.baitursinov.caravan.event.sourcing.entity.stream;

import dev.baitursinov.caravan.entity.EntityReference;

import java.time.ZonedDateTime;

import static java.util.Objects.requireNonNull;

/**
 * One entry of the entity stream: an entityReference and the timestamp of its first event, i.e. entities
 * creation time.
 */
public record EntityStreamEntry(EntityReference entityReference,
                                ZonedDateTime firstEventTimestamp) {

  public EntityStreamEntry {
    requireNonNull(entityReference, "entityReference cannot be null");
    requireNonNull(firstEventTimestamp, "firstEventTimestamp cannot be null");
  }
}
