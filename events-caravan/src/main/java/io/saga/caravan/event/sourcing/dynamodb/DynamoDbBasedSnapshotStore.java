package io.saga.caravan.event.sourcing.dynamodb;

import io.saga.caravan.entity.EntityReference;
import io.saga.caravan.event.sourcing.snapshot.EntitySnapshot;
import io.saga.caravan.event.sourcing.snapshot.SnapshotDeserializer;
import io.saga.caravan.event.sourcing.snapshot.SnapshotSerializer;
import io.saga.caravan.event.sourcing.snapshot.SnapshotStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;
import java.util.Optional;

@Component
public class DynamoDbBasedSnapshotStore implements SnapshotStore {

  private static final String ENTITY_REFERENCE_KEY = "entityReference";
  private static final String VERSION_KEY = "version";
  private static final String PAYLOAD_KEY = "payload";

  private final SnapshotSerializer snapshotSerializer;
  private final SnapshotDeserializer snapshotDeserializer;
  private final DynamoDbClient dynamoDbClient;
  private final String snapshotsTableName;

  public DynamoDbBasedSnapshotStore(
      SnapshotSerializer snapshotSerializer,
      SnapshotDeserializer snapshotDeserializer,
      DynamoDbClient dynamoDbClient,
      @Value("${caravan.event.sourcing.snapshot-store.dynamo-db.table-name}") String snapshotsTableName) {

    this.snapshotSerializer = snapshotSerializer;
    this.snapshotDeserializer = snapshotDeserializer;
    this.dynamoDbClient = dynamoDbClient;
    this.snapshotsTableName = snapshotsTableName;
  }

  @Override
  public void save(EntitySnapshot<?> snapshot) {
    Map<String, AttributeValue> item = Map.of(
        ENTITY_REFERENCE_KEY, AttributeValue.fromS(toEventReferenceStringValue(snapshot.entityReference())),
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
            .key(Map.of(ENTITY_REFERENCE_KEY, AttributeValue.fromS(toEventReferenceStringValue(entityReference))))
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

  private String toEventReferenceStringValue(EntityReference entityReference) {
    return entityReference.entityName() + "#" + entityReference.entityId();
  }
}
