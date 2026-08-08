package dev.baitursinov.caravan.event.sourcing.dynamodb;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.sourcing.snapshot.EntitySnapshot;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotDeserializer;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotSerializer;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotStore;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;
import java.util.Optional;

import static dev.baitursinov.caravan.event.sourcing.dynamodb.DynamoDbUtils.requireTableIsActive;
import static dev.baitursinov.caravan.event.sourcing.dynamodb.PrimaryKeyUtils.toPartitionKeyValue;
import static dev.baitursinov.caravan.utils.TextUtils.hasText;
import static java.util.Objects.requireNonNull;

@Slf4j
public class DynamoDbBasedSnapshotStore implements SnapshotStore {

  private static final String ENTITY_REFERENCE_KEY = "entityReference";
  private static final String VERSION_KEY = "version";
  private static final String PAYLOAD_KEY = "payload";

  private final DynamoDbClient dynamoDbClient;
  private final String snapshotsTableName;
  private final boolean consistentRead;
  private final SnapshotSerializer snapshotSerializer;
  private final SnapshotDeserializer snapshotDeserializer;

  public DynamoDbBasedSnapshotStore(DynamoDbClient dynamoDbClient,
                                    String snapshotsTableName,
                                    boolean consistentRead,
                                    SnapshotSerializer snapshotSerializer,
                                    SnapshotDeserializer snapshotDeserializer) {
    requireNonNull(dynamoDbClient);
    requireNonNull(snapshotSerializer);
    requireNonNull(snapshotDeserializer);

    if (!hasText(snapshotsTableName)) {
      throw new DynamoDbSetupException("snapshotsTableName must be set");
    }
    requireTableIsActive(dynamoDbClient, snapshotsTableName);

    this.dynamoDbClient = dynamoDbClient;
    this.snapshotsTableName = snapshotsTableName;
    this.consistentRead = consistentRead;
    this.snapshotSerializer = snapshotSerializer;
    this.snapshotDeserializer = snapshotDeserializer;
  }

  @Override
  public void save(EntitySnapshot<?> snapshot) {
    Map<String, AttributeValue> item = Map.of(
        ENTITY_REFERENCE_KEY, AttributeValue.fromS(toPartitionKeyValue(snapshot.entityReference())),
        VERSION_KEY, AttributeValue.fromN(String.valueOf(snapshot.version())),
        PAYLOAD_KEY, AttributeValue.fromS(snapshotSerializer.serializePayload(snapshot)));

    dynamoDbClient.putItem(
        PutItemRequest.builder()
            .tableName(snapshotsTableName)
            .item(item)
            .build());

    log.debug("Saved snapshot of {} at version={}",
        snapshot.entityReference(), snapshot.version());
  }

  @Override
  public <S> Optional<EntitySnapshot<S>> load(EntityReference entityReference,
                                              Class<S> snapshotClass) {
    GetItemResponse response = dynamoDbClient.getItem(
        GetItemRequest.builder()
            .tableName(snapshotsTableName)
            .key(Map.of(ENTITY_REFERENCE_KEY, AttributeValue.fromS(toPartitionKeyValue(entityReference))))
            .consistentRead(consistentRead)
            .build());

    if (!response.hasItem() || response.item().isEmpty()) {
      log.debug("No snapshot found for {}", entityReference);
      return Optional.empty();
    }

    Map<String, AttributeValue> item = response.item();

    S payload = snapshotDeserializer.deserializePayload(item.get(PAYLOAD_KEY).s(), snapshotClass);

    long version = Long.parseLong(item.get(VERSION_KEY).n());

    log.debug("Loaded snapshot of {} at version={}", entityReference, version);

    return Optional.of(
        EntitySnapshot.<S>builder()
            .entityReference(entityReference)
            .version(version)
            .payload(payload)
            .build());
  }
}
