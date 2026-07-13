package io.saga.caravan.event.sourcing;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.Event;

import java.util.stream.Stream;

public interface EventStore {

  Stream<Event<?>> getEventsOfEntity(EntityReference entityReference,
                                     long fromSequenceNumberExclusive);
}
