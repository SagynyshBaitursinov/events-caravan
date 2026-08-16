package dev.baitursinov.caravan.autoconfigure.dynamodb;

import dev.baitursinov.caravan.event.producer.EventProducer;
import dev.baitursinov.caravan.event.serialization.EventPayloadDeserializer;
import dev.baitursinov.caravan.event.serialization.EventPayloadSerializer;
import dev.baitursinov.caravan.event.sourcing.EventStore;
import dev.baitursinov.caravan.event.sourcing.dynamodb.DynamoDbBasedEventStore;
import dev.baitursinov.caravan.event.sourcing.dynamodb.DynamoDbBasedSnapshotStore;
import dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream.DynamoDbBasedEntityStreamWriter;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamWriter;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotDeserializer;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotSerializer;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CaravanDynamoDbAutoConfigurationTest {

  AutoConfigurations autoConfigurations = AutoConfigurations.of(CaravanDynamoDbAutoConfiguration.class);

  static DynamoDbClient dynamoDbClientWithActiveTable() {
    var dynamoDbClient = mock(DynamoDbClient.class);
    when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(TableDescription.builder().tableStatus(TableStatus.ACTIVE).build())
                .build());
    return dynamoDbClient;
  }

  ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(autoConfigurations)
      .withUserConfiguration(ApplicationConfiguration.class)
      .withPropertyValues(
          "caravan.event.sourcing.event-store.dynamo-db.table-name=test-app_events",
          "caravan.event.sourcing.event-store.dynamo-db.query-max-page-size=50",
          "caravan.event.sourcing.event-store.dynamo-db.partition-shard-size=100",
          "caravan.event.sourcing.snapshot-store.dynamo-db.table-name=test-app_snapshots");

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfiguration {

    @Bean
    EventPayloadSerializer eventPayloadSerializer() {
      return mock(EventPayloadSerializer.class);
    }

    @Bean
    EventPayloadDeserializer eventPayloadDeserializer() {
      return mock(EventPayloadDeserializer.class);
    }

    @Bean
    SnapshotSerializer snapshotSerializer() {
      return mock(SnapshotSerializer.class);
    }

    @Bean
    SnapshotDeserializer snapshotDeserializer() {
      return mock(SnapshotDeserializer.class);
    }

    @Bean
    DynamoDbClient dynamoDbClient() {
      return dynamoDbClientWithActiveTable();
    }
  }

  @Test
  void contributesBothStores() {
    contextRunner.run(context -> {
      assertThat(context).hasSingleBean(DynamoDbBasedEventStore.class);
      assertThat(context).hasSingleBean(DynamoDbBasedSnapshotStore.class);
    });
  }

  @Test
  void bindsEventStoreProperties() {
    contextRunner
        .run(context ->
            assertThat(context).getBean(DynamoDbEventStoreConfigurationProperties.class)
                .satisfies(properties -> {
                  assertThat(properties.tableName()).isEqualTo("test-app_events");
                  assertThat(properties.queryMaxPageSize()).isEqualTo(50);
                  assertThat(properties.partitionShardSize()).isEqualTo(100);
                }));
  }

  @Test
  void leavesPartitionShardSizeAndPageSizesAtDefaults() {
    new ApplicationContextRunner()
        .withConfiguration(autoConfigurations)
        .withUserConfiguration(ApplicationConfiguration.class)
        .withPropertyValues(
            "caravan.event.sourcing.event-store.dynamo-db.table-name=test-app_events",
            "caravan.event.sourcing.snapshot-store.dynamo-db.table-name=test-app_snapshots")
        .run(context ->
            assertThat(context).getBean(DynamoDbEventStoreConfigurationProperties.class)
                .satisfies(properties -> {
                  assertThat(properties.partitionShardSize()).isEqualTo(10_000);
                  assertThat(properties.queryMaxPageSize()).isEqualTo(1_000);
                }));
  }

  @Test
  void failsWhenDynamoDbClientIsNotProvided() {
    new ApplicationContextRunner()
        .withConfiguration(autoConfigurations)
        .run(context -> assertThat(context).hasFailed());
  }

  @Test
  void failsWhenNoTableNamesAreConfigured() {
    new ApplicationContextRunner()
        .withConfiguration(autoConfigurations)
        .withUserConfiguration(ApplicationConfiguration.class)
        .run(context -> assertThat(context).hasFailed());
  }

  @Test
  void failsWhenPartitionShardSizeIsNotPositive() {
    contextRunner
        .withPropertyValues("caravan.event.sourcing.event-store.dynamo-db.partition-shard-size=0")
        .run(context -> assertThat(context).hasFailed());
  }

  @Test
  void failsWhenQueryMaxPageSizeIsNotPositive() {
    contextRunner
        .withPropertyValues("caravan.event.sourcing.event-store.dynamo-db.query-max-page-size=0")
        .run(context -> assertThat(context).hasFailed());
  }

  @Test
  void failsWhenEventsTableDoesNotExist() {
    new ApplicationContextRunner()
        .withConfiguration(autoConfigurations)
        .withUserConfiguration(ApplicationConfigurationWithMissingTable.class)
        .withPropertyValues(
            "caravan.event.sourcing.event-store.dynamo-db.table-name=test-app_events",
            "caravan.event.sourcing.snapshot-store.dynamo-db.table-name=test-app_snapshots")
        .run(context -> assertThat(context).hasFailed());
  }

  @Test
  void failsWhenEventsTableIsNotActive() {
    new ApplicationContextRunner()
        .withConfiguration(autoConfigurations)
        .withUserConfiguration(ApplicationConfigurationWithInactiveTable.class)
        .withPropertyValues(
            "caravan.event.sourcing.event-store.dynamo-db.table-name=test-app_events",
            "caravan.event.sourcing.snapshot-store.dynamo-db.table-name=test-app_snapshots")
        .run(context -> assertThat(context).hasFailed());
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfigurationWithMissingTable extends ApplicationConfiguration {

    @Bean
    @Override
    DynamoDbClient dynamoDbClient() {
      var dynamoDbClient = mock(DynamoDbClient.class);
      when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
          .thenThrow(ResourceNotFoundException.builder().message("table not found").build());
      return dynamoDbClient;
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfigurationWithInactiveTable extends ApplicationConfiguration {

    @Bean
    @Override
    DynamoDbClient dynamoDbClient() {
      var dynamoDbClient = mock(DynamoDbClient.class);
      when(dynamoDbClient.describeTable(any(DescribeTableRequest.class)))
          .thenReturn(
              DescribeTableResponse.builder()
                  .table(TableDescription.builder().tableStatus(TableStatus.CREATING).build())
                  .build());
      return dynamoDbClient;
    }
  }

  @Test
  void applicationMaySupplyItsOwnStores() {
    var ownEventStore = mock(EventStore.class);
    var ownSnapshotStore = mock(SnapshotStore.class);
    var ownEventProducer = mock(EventProducer.class);

    contextRunner
        .withBean(EventStore.class, () -> ownEventStore)
        .withBean(SnapshotStore.class, () -> ownSnapshotStore)
        .withBean(EventProducer.class, () -> ownEventProducer)
        .run(context -> {
          assertThat(context).doesNotHaveBean(DynamoDbBasedEventStore.class);
          assertThat(context).doesNotHaveBean(DynamoDbBasedSnapshotStore.class);
        });
  }

  @Test
  void applicationMaySupplyOwnEventProducer() {
    var ownEventProducer = mock(EventProducer.class);

    contextRunner
        .withBean(EventProducer.class, () -> ownEventProducer)
        .run(context -> {
          assertThat(context).doesNotHaveBean(DynamoDbBasedEventStore.class);
          assertThat(context).hasSingleBean(DynamoDbBasedSnapshotStore.class);
        });
  }

  @Test
  void doesNotConfigureEntityStreamByDefault() {
    contextRunner.run(context -> assertThat(context).doesNotHaveBean(DynamoDbBasedEntityStreamWriter.class));
  }

  @Test
  void configuresEntityStreamWhenTableNameConfigured() {
    contextRunner
        .withPropertyValues("caravan.event.sourcing.entity-stream.dynamo-db.table-name=test-app_entity-stream")
        .run(context -> assertThat(context).hasSingleBean(DynamoDbBasedEntityStreamWriter.class));
  }

  @Test
  void bindsEntityStreamProperties() {
    contextRunner
        .withPropertyValues("caravan.event.sourcing.entity-stream.dynamo-db.table-name=test-app_entity-stream")
        .run(context ->
            assertThat(context).getBean(DynamoDbEntityStreamConfigurationProperties.class)
                .satisfies(properties ->
                    assertThat(properties.tableName()).isEqualTo("test-app_entity-stream")));
  }

  @Test
  void applicationMaySupplyOwnEntityStreamWriter() {
    var ownEntityStreamWriter = mock(EntityStreamWriter.class);

    contextRunner
        .withPropertyValues("caravan.event.sourcing.entity-stream.dynamo-db.table-name=test-app_entity-stream")
        .withBean(EntityStreamWriter.class, () -> ownEntityStreamWriter)
        .run(context -> assertThat(context).doesNotHaveBean(DynamoDbBasedEntityStreamWriter.class));
  }
}
