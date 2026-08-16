package dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.sourcing.dynamodb.DynamoDbSetupException;
import dev.baitursinov.caravan.event.sourcing.dynamodb.DynamoDbStoreException;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DynamoDbBasedEntityStreamWriterTest {

  private static final EntityReference ENTITY_REFERENCE = new EntityReference("Entity", "reference-1");
  private static final ZonedDateTime FIRST_EVENT_TIMESTAMP = ZonedDateTime.parse("2026-08-10T14:03:22.123Z");

  DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);

  DynamoDbBasedEntityStreamWriter entityStream;

  @BeforeEach
  void setUp() {
    when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableStatus(TableStatus.ACTIVE).build())
                .build());

    entityStream = new DynamoDbBasedEntityStreamWriter(dynamoDbClient, "entity-stream-table");

    clearInvocations(dynamoDbClient);
  }

  @Test
  void writingPutsItemUnconditionallyKeyedByNameBucketAndShard() {
    entityStream.write(new EntityStreamEntry(ENTITY_REFERENCE, FIRST_EVENT_TIMESTAMP), "2026-08", 3);

    ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
    verify(dynamoDbClient).putItem(captor.capture());

    PutItemRequest issuedPut = captor.getValue();
    assertThat(issuedPut.tableName()).isEqualTo("entity-stream-table");
    assertThat(issuedPut.conditionExpression()).isNull();

    assertThat(issuedPut.item().get("PK").s()).isEqualTo("Entity#2026-08#3");
    assertThat(issuedPut.item().get("SK").s())
        .isEqualTo("2026-08-10T14:03:22.123Z#reference-1");
  }

  @Test
  void writingTheSameEntryAndLocationTwiceProducesTheSameItem() {
    entityStream.write(new EntityStreamEntry(ENTITY_REFERENCE, FIRST_EVENT_TIMESTAMP), "2026-08", 3);
    entityStream.write(new EntityStreamEntry(ENTITY_REFERENCE, FIRST_EVENT_TIMESTAMP), "2026-08", 3);

    ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
    verify(dynamoDbClient, times(2)).putItem(captor.capture());

    assertThat(captor.getAllValues().get(0).item()).isEqualTo(captor.getAllValues().get(1).item());
  }

  @Test
  void rejectsEntityReferenceContainingSeparator() {
    var entryWithSeparator =
        new EntityStreamEntry(new EntityReference("Entity#Name", "1"), FIRST_EVENT_TIMESTAMP);

    assertThatThrownBy(() -> entityStream.write(entryWithSeparator, "2026-08", 3))
        .isExactlyInstanceOf(DynamoDbStoreException.class);
  }

  @Test
  void requiresTableName() {
    assertThatThrownBy(() -> new DynamoDbBasedEntityStreamWriter(dynamoDbClient, " "))
        .isExactlyInstanceOf(DynamoDbSetupException.class);
  }

  @Test
  void requiresTableToExist() {
    when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
        .thenThrow(ResourceNotFoundException.builder().build());

    assertThatThrownBy(() -> new DynamoDbBasedEntityStreamWriter(dynamoDbClient, "missing-table"))
        .isExactlyInstanceOf(DynamoDbSetupException.class);
  }
}
