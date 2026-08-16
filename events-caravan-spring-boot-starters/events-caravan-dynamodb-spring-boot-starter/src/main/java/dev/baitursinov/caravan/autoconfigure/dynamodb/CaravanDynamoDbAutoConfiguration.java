package dev.baitursinov.caravan.autoconfigure.dynamodb;

import dev.baitursinov.caravan.autoconfigure.CaravanEventDrivenComponentsAutoConfiguration;
import dev.baitursinov.caravan.autoconfigure.CaravanEventSourcingAutoConfiguration;
import dev.baitursinov.caravan.autoconfigure.CaravanJacksonSerializationAutoConfiguration;
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
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Slf4j
@AutoConfiguration(
    before = {CaravanEventSourcingAutoConfiguration.class, CaravanEventDrivenComponentsAutoConfiguration.class},
    after = CaravanJacksonSerializationAutoConfiguration.class)
@EnableConfigurationProperties({
    DynamoDbEventStoreConfigurationProperties.class,
    DynamoDbSnapshotStoreConfigurationProperties.class,
    DynamoDbEntityStreamConfigurationProperties.class
})
public class CaravanDynamoDbAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean({EventStore.class, EventProducer.class})
  public DynamoDbBasedEventStore dynamoDbBasedEventStore(
      DynamoDbClient dynamoDbClient,
      EventPayloadSerializer eventPayloadSerializer,
      EventPayloadDeserializer eventPayloadDeserializer,
      DynamoDbEventStoreConfigurationProperties properties) {

    log.info(
        "Configuring DynamoDbBasedEventStore on tableName={} "
            + "(queryMaxPageSize={}, partitionShardSize={}, consistentRead={})",
        properties.tableName(), properties.queryMaxPageSize(), properties.partitionShardSize(),
        properties.consistentRead());

    return new DynamoDbBasedEventStore(
        dynamoDbClient,
        properties.tableName(),
        properties.queryMaxPageSize(),
        properties.partitionShardSize(),
        properties.consistentRead(),
        eventPayloadSerializer,
        eventPayloadDeserializer);
  }

  @Bean
  @ConditionalOnMissingBean(SnapshotStore.class)
  public DynamoDbBasedSnapshotStore dynamoDbBasedSnapshotStore(
      DynamoDbClient dynamoDbClient,
      SnapshotSerializer snapshotSerializer,
      SnapshotDeserializer snapshotDeserializer,
      DynamoDbSnapshotStoreConfigurationProperties properties) {

    log.info("Configuring DynamoDbBasedSnapshotStore on tableName={} (consistentRead={})",
        properties.tableName(), properties.consistentRead());

    return new DynamoDbBasedSnapshotStore(
        dynamoDbClient,
        properties.tableName(),
        properties.consistentRead(),
        snapshotSerializer,
        snapshotDeserializer);
  }

  @Bean
  @ConditionalOnProperty(prefix = DynamoDbEntityStreamConfigurationProperties.PREFIX, name = "table-name")
  @ConditionalOnMissingBean(EntityStreamWriter.class)
  public DynamoDbBasedEntityStreamWriter dynamoDbBasedEntityStream(
      DynamoDbClient dynamoDbClient,
      DynamoDbEntityStreamConfigurationProperties properties) {

    log.info("Configuring DynamoDbBasedEntityStream on tableName={}", properties.tableName());

    return new DynamoDbBasedEntityStreamWriter(dynamoDbClient, properties.tableName());
  }
}
