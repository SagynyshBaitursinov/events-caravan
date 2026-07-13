package io.saga.caravan.event.sourcing;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component
public class EventSourcedEntityNamesKeeper {

  private final Set<String> entityNames = new HashSet<>();
  private final Map<Class<? extends EventSourcedEntity>, String> entityClassToEntityName = new HashMap<>();

  public void register(String entityName,
                       Class<? extends EventSourcedEntity> entityClass) {
    if (entityNames.contains(entityName)
        || entityClassToEntityName.containsKey(entityClass)) {
      throw new EventSourcedEntitySetupException(
          "entityName=%s or entityClass=%s are duplicated"
              .formatted(entityName, entityClass));
    }

    entityNames.add(entityName);
    entityClassToEntityName.put(entityClass, entityName);
  }

  public Map<Class<? extends EventSourcedEntity>, String> getEntityClassToEntityNameMap() {
    return Collections.unmodifiableMap(entityClassToEntityName);
  }

  public Set<String> getEntityNames() {
    return Collections.unmodifiableSet(entityNames);
  }
}
