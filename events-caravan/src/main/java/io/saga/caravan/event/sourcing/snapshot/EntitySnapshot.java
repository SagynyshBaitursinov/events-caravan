package io.saga.caravan.event.sourcing.snapshot;

import io.saga.caravan.entity.EntityReference;
import lombok.Builder;

@Builder
public record EntitySnapshot<S>(EntityReference entityReference,
                                long version,
                                S payload) {

}
