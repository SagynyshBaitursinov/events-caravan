package io.saga.caravan.event.sourcing.snapshot;

import io.saga.caravan.event.sourcing.EventSourcedEntity;
import io.saga.caravan.event.sourcing.EventSourcedEntitySetupException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class SnapshotTakersConfiguration {

  @Bean
  public Map<Class<? extends EventSourcedEntity>, SnapshotTaker<? extends EventSourcedEntity, ?>> snapshotTakerMap(
      List<SnapshotTaker<? extends EventSourcedEntity, ?>> snapshotTakers) {

    var snapshotTakerMap = new HashMap<Class<? extends EventSourcedEntity>, SnapshotTaker<? extends EventSourcedEntity, ?>>();

    snapshotTakers.forEach(snapshotTaker -> {
      if (snapshotTakerMap.containsKey(snapshotTaker.entityClass())) {
        throw new EventSourcedEntitySetupException(
            "Duplicate snapshot taker found for class %s"
                .formatted(snapshotTaker.entityClass()));
      }
      snapshotTakerMap.put(snapshotTaker.entityClass(), snapshotTaker);
    });

    return snapshotTakerMap;
  }
}
