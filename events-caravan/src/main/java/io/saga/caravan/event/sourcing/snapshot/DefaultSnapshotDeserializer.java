package io.saga.caravan.event.sourcing.snapshot;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Component
@RequiredArgsConstructor
public class DefaultSnapshotDeserializer implements SnapshotDeserializer {

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
