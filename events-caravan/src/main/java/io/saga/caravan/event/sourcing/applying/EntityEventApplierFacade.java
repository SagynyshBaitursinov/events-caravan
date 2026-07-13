package io.saga.caravan.event.sourcing.applying;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.sourcing.EntityEventApplier;
import io.saga.caravan.event.sourcing.EventSourcedEntity;
import org.jspecify.annotations.Nullable;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Component
public class EntityEventApplierFacade {

  @Nullable
  private static EntityEventApplier entityEventApplier;

  public EntityEventApplierFacade(@Lazy EntityEventApplier entityEventApplier) {
    EntityEventApplierFacade.entityEventApplier = entityEventApplier;
  }

  public static void apply(EventSourcedEntity eventSourcedEntity,
                           Event<?> event) {
    if (entityEventApplier == null) {
      throw new EventApplyingException(
          "Cannot apply event as entityEventApplier is not initialized");
    }

    entityEventApplier.apply(eventSourcedEntity, event);
  }
}
