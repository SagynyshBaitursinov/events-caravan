package io.saga.caravan.autoconfigure.dynamodb;

import io.saga.caravan.autoconfigure.CaravanEventDrivenComponentsAutoConfiguration;
import io.saga.caravan.autoconfigure.CaravanEventSourcingAutoConfiguration;
import io.saga.caravan.autoconfigure.CaravanJacksonSerializationAutoConfiguration;
import io.saga.caravan.event.producer.EventProducer;
import io.saga.caravan.event.serialization.EventPayloadDeserializer;
import io.saga.caravan.event.serialization.EventPayloadSerializer;
import io.saga.caravan.event.sourcing.EventStore;
import io.saga.caravan.event.sourcing.dynamodb.DynamoDbBasedEventStore;
import io.saga.caravan.event.sourcing.dynamodb.DynamoDbBasedSnapshotStore;
import io.saga.caravan.event.sourcing.snapshot.SnapshotDeserializer;
import io.saga.caravan.event.sourcing.snapshot.SnapshotSerializer;
import io.saga.caravan.event.sourcing.snapshot.SnapshotStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@AutoConfiguration(
    before = {CaravanEventSourcingAutoConfiguration.class, CaravanEventDrivenComponentsAutoConfiguration.class},
    after = CaravanJacksonSerializationAutoConfiguration.class)
@EnableConfigurationProperties({
    DynamoDbEventStoreProperties.class,
    DynamoDbSnapshotStoreProperties.class
})
public class CaravanDynamoDbAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean({EventStore.class, EventProducer.class})
  public DynamoDbBasedEventStore dynamoDbBasedEventStore(
      DynamoDbClient dynamoDbClient,
      EventPayloadSerializer eventPayloadSerializer,
      EventPayloadDeserializer eventPayloadDeserializer,
      DynamoDbEventStoreProperties properties) {

    return new DynamoDbBasedEventStore(
        dynamoDbClient,
        eventPayloadSerializer,
        eventPayloadDeserializer,
        properties.tableName(),
        properties.queryMaxPageSize(),
        properties.partitionShardSize());
  }

  @Bean
  @ConditionalOnMissingBean(SnapshotStore.class)
  public DynamoDbBasedSnapshotStore dynamoDbBasedSnapshotStore(
      DynamoDbClient dynamoDbClient,
      SnapshotSerializer snapshotSerializer,
      SnapshotDeserializer snapshotDeserializer,
      DynamoDbSnapshotStoreProperties properties) {

    return new DynamoDbBasedSnapshotStore(
        dynamoDbClient,
        snapshotSerializer,
        snapshotDeserializer,
        properties.tableName());
  }
}
