package io.saga.caravan.event.sourcing.snapshot;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Component
@RequiredArgsConstructor
public class DefaultSnapshotSerializer implements SnapshotSerializer {

  private final JsonMapper jsonMapper;

  @Override
  public String serializePayload(EntitySnapshot<?> entitySnapshot) {
    try {
      return jsonMapper.writeValueAsString(entitySnapshot.payload());
    } catch (JacksonException jacksonException) {
      throw new SnapshotException(
          "Could not serialize snapshot for %s, version=%s"
              .formatted(
                  entitySnapshot.entityReference(),
                  entitySnapshot.version()));
    }
  }
}
