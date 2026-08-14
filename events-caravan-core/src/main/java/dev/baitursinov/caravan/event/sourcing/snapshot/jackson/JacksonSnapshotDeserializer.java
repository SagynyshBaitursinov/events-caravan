package dev.baitursinov.caravan.event.sourcing.snapshot.jackson;

import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotDeserializer;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotException;
import lombok.RequiredArgsConstructor;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@RequiredArgsConstructor
public class JacksonSnapshotDeserializer implements SnapshotDeserializer {

  private final JsonMapper jsonMapper;

  @Override
  public <S> S deserializePayload(String snapshotPayloadAsJson,
                                  Class<S> snapshotPayloadClass) {
    try {
      return jsonMapper.readValue(snapshotPayloadAsJson, snapshotPayloadClass);
    } catch (JacksonException jacksonException) {
      throw new SnapshotException(
          "Could not deserialize payload into class=%s"
              .formatted(snapshotPayloadClass.getName()));
    }
  }
}
