package io.saga.caravan.event.sourcing.dynamodb;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DynamoDbUtilsTest {

  DynamoDbClient dynamoDbClient = mock(DynamoDbClient.class);

  @Test
  void acceptsAnActiveTable() {
    when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableStatus(TableStatus.ACTIVE).build())
                .build());

    assertThatNoException()
        .isThrownBy(() -> DynamoDbUtils.requireTableIsActive(dynamoDbClient, "events-table"));
  }

  @Test
  void rejectsTableThatDoesNotExist() {
    when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
        .thenThrow(ResourceNotFoundException.builder().message("not found").build());

    assertThatThrownBy(() -> DynamoDbUtils.requireTableIsActive(dynamoDbClient, "events-table"))
        .isExactlyInstanceOf(DynamoDbSetupException.class)
        .hasMessageContaining("events-table");
  }

  @Test
  void rejectsATableThatIsNotActive() {
    when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableStatus(TableStatus.CREATING).build())
                .build());

    assertThatThrownBy(() -> DynamoDbUtils.requireTableIsActive(dynamoDbClient, "events-table"))
        .isExactlyInstanceOf(DynamoDbSetupException.class)
        .hasMessageContaining("events-table")
        .hasMessageContaining("CREATING");
  }
}
