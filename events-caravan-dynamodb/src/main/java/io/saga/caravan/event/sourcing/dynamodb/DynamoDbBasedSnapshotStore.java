package io.saga.caravan.event.sourcing.dynamodb;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.sourcing.snapshot.EntitySnapshot;
import io.saga.caravan.event.sourcing.snapshot.SnapshotDeserializer;
import io.saga.caravan.event.sourcing.snapshot.SnapshotSerializer;
import io.saga.caravan.event.sourcing.snapshot.SnapshotStore;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;
import java.util.Optional;

import static io.saga.caravan.event.sourcing.dynamodb.PrimaryKeyUtils.toPartitionKeyValue;
import static io.saga.caravan.utils.TextUtils.hasText;
import static java.util.Objects.requireNonNull;

public class DynamoDbBasedSnapshotStore implements SnapshotStore {

  private static final String ENTITY_REFERENCE_KEY = "entityReference";
  private static final String VERSION_KEY = "version";
  private static final String PAYLOAD_KEY = "payload";

  private final DynamoDbClient dynamoDbClient;
  private final SnapshotSerializer snapshotSerializer;
  private final SnapshotDeserializer snapshotDeserializer;
  private final String snapshotsTableName;

  public DynamoDbBasedSnapshotStore(DynamoDbClient dynamoDbClient,
                                    SnapshotSerializer snapshotSerializer,
                                    SnapshotDeserializer snapshotDeserializer,
                                    String snapshotsTableName) {
    requireNonNull(dynamoDbClient);
    requireNonNull(snapshotSerializer);
    requireNonNull(snapshotDeserializer);

    if (!hasText(snapshotsTableName)) {
      throw new IllegalArgumentException("snapshotsTableName must be set");
    }

    this.snapshotSerializer = snapshotSerializer;
    this.snapshotDeserializer = snapshotDeserializer;
    this.dynamoDbClient = dynamoDbClient;
    this.snapshotsTableName = snapshotsTableName;
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
  }

  @Override
  public <S> Optional<EntitySnapshot<S>> load(EntityReference entityReference,
                                              Class<S> snapshotClass) {
    GetItemResponse response = dynamoDbClient.getItem(
        GetItemRequest.builder()
            .tableName(snapshotsTableName)
            .key(Map.of(ENTITY_REFERENCE_KEY, AttributeValue.fromS(toPartitionKeyValue(entityReference))))
            .build());

    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }

    Map<String, AttributeValue> item = response.item();

    S payload = snapshotDeserializer.deserializePayload(item.get(PAYLOAD_KEY).s(), snapshotClass);

    long version = Long.parseLong(item.get(VERSION_KEY).n());

    return Optional.of(
        EntitySnapshot.<S>builder()
            .entityReference(entityReference)
            .version(version)
            .payload(payload)
            .build());
  }
}
