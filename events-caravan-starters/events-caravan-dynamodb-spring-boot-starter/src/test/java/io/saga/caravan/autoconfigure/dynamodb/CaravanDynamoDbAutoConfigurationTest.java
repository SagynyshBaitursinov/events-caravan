package io.saga.caravan.autoconfigure.dynamodb;

import io.saga.caravan.autoconfigure.CaravanMessagingConfigurationProperties;
import io.saga.caravan.event.consumer.EventMessageConsumer;
import io.saga.caravan.event.consumer.queue.SubscribedEntityQueueNamesKeeper;
import io.saga.caravan.event.producer.EventProducer;
import io.saga.caravan.event.serialization.EventPayloadDeserializer;
import io.saga.caravan.event.serialization.EventPayloadSerializer;
import io.saga.caravan.event.sourcing.EventStore;
import io.saga.caravan.event.sourcing.dynamodb.DynamoDbBasedEventStore;
import io.saga.caravan.event.sourcing.dynamodb.DynamoDbBasedSnapshotStore;
import io.saga.caravan.event.sourcing.snapshot.SnapshotDeserializer;
import io.saga.caravan.event.sourcing.snapshot.SnapshotSerializer;
import io.saga.caravan.event.sourcing.snapshot.SnapshotStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class CaravanDynamoDbAutoConfigurationTest {

  AutoConfigurations autoConfigurations = AutoConfigurations.of(CaravanDynamoDbAutoConfiguration.class);

  ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(autoConfigurations)
      .withUserConfiguration(ApplicationConfiguration.class)
      .withPropertyValues(
          "caravan.event.store.dynamo-db.table-name=test-app_events",
          "caravan.event.store.dynamo-db.query-max-page-size=50",
          "caravan.event.store.dynamo-db.partition-shard-size=100",
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
      return mock(DynamoDbClient.class);
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
            assertThat(context).getBean(DynamoDbEventStoreProperties.class)
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
            "caravan.event.store.dynamo-db.table-name=test-app_events",
            "caravan.event.sourcing.snapshot-store.dynamo-db.table-name=test-app_snapshots")
        .run(context ->
            assertThat(context).getBean(DynamoDbEventStoreProperties.class)
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
        .withPropertyValues("caravan.event.store.dynamo-db.partition-shard-size=0")
        .run(context -> assertThat(context).hasFailed());
  }

  @Test
  void failsWhenQueryMaxPageSizeIsNotPositive() {
    contextRunner
        .withPropertyValues("caravan.event.store.dynamo-db.query-max-page-size=0")
        .run(context -> assertThat(context).hasFailed());
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
}
