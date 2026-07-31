package io.saga.caravan.event.sourcing.snapshot.jackson;

import io.saga.caravan.event.sourcing.snapshot.EntitySnapshot;
import io.saga.caravan.event.sourcing.snapshot.SnapshotException;
import io.saga.caravan.event.sourcing.snapshot.SnapshotSerializer;
import lombok.RequiredArgsConstructor;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@RequiredArgsConstructor
public class JacksonSnapshotSerializer implements SnapshotSerializer {

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
