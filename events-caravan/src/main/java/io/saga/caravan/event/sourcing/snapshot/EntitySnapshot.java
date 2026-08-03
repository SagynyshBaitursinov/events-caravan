package io.saga.caravan.event.sourcing.snapshot;

import io.saga.caravan.entity.EntityReference;
import lombok.Builder;

/**
 * A point-in-time capture of an {@code EventSourcedEntity}'s state at the given
 * {@code version}, so it can be restored without replaying every event from the beginning.
 * Produced by a {@code SnapshotTaker} and persisted/loaded via a {@link SnapshotStore}.
 *
 * @param entityReference identifies the entity this snapshot was taken of
 * @param version         the sequence number of the last event reflected in {@code payload}
 * @param payload         the entity's captured state
 * @param <S>             the type of the captured state
 */
@Builder
public record EntitySnapshot<S>(EntityReference entityReference,
                                long version,
                                S payload) {

}
