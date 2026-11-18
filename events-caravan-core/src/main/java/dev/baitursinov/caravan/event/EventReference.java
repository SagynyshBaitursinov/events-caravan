package dev.baitursinov.caravan.event;

import dev.baitursinov.caravan.entity.EntityReference;

/**
 * Identifies a single {@link Event}, without its payload: an event named {@code eventName}
 * recorded against {@code entityReference}, told apart from the entity's other events either by
 * its position in the entity's sequence ({@link SequencedEventReference}) or by an id of its own
 * ({@link UnsequencedEventReference}).
 */
public sealed interface EventReference permits SequencedEventReference, UnsequencedEventReference {

  EntityReference entityReference();

  String eventName();
}
