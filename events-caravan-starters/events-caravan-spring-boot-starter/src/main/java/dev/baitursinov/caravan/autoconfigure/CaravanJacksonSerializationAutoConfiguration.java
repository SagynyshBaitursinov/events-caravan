package dev.baitursinov.caravan.autoconfigure;

import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.serialization.EventDeserializer;
import dev.baitursinov.caravan.event.serialization.EventPayloadDeserializer;
import dev.baitursinov.caravan.event.serialization.EventPayloadSerializer;
import dev.baitursinov.caravan.event.serialization.EventSerializer;
import dev.baitursinov.caravan.event.serialization.jackson.JacksonEventDeserializer;
import dev.baitursinov.caravan.event.serialization.jackson.JacksonEventPayloadDeserializer;
import dev.baitursinov.caravan.event.serialization.jackson.JacksonEventPayloadSerializer;
import dev.baitursinov.caravan.event.serialization.jackson.JacksonEventSerializer;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotDeserializer;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotSerializer;
import dev.baitursinov.caravan.event.sourcing.snapshot.jackson.JacksonSnapshotDeserializer;
import dev.baitursinov.caravan.event.sourcing.snapshot.jackson.JacksonSnapshotSerializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.json.JsonMapper;

@AutoConfiguration(
    after = CaravanEventRegistryAutoConfiguration.class,
    afterName = "org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration")
@ConditionalOnClass(JsonMapper.class)
public class CaravanJacksonSerializationAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public EventPayloadSerializer eventPayloadSerializer(JsonMapper jsonMapper) {
    return new JacksonEventPayloadSerializer(jsonMapper);
  }

  @Bean
  @ConditionalOnMissingBean
  public EventPayloadDeserializer eventPayloadDeserializer(JsonMapper jsonMapper,
                                                           EntityEventsRegistry entityEventsRegistry) {
    return new JacksonEventPayloadDeserializer(jsonMapper, entityEventsRegistry);
  }

  @Bean
  @ConditionalOnMissingBean
  public EventSerializer eventSerializer(JsonMapper jsonMapper) {
    return new JacksonEventSerializer(jsonMapper);
  }

  @Bean
  @ConditionalOnMissingBean
  public EventDeserializer eventDeserializer(JsonMapper jsonMapper,
                                             EventPayloadDeserializer eventPayloadDeserializer) {
    return new JacksonEventDeserializer(jsonMapper, eventPayloadDeserializer);
  }

  @Bean
  @ConditionalOnMissingBean
  public SnapshotSerializer snapshotSerializer(JsonMapper jsonMapper) {
    return new JacksonSnapshotSerializer(jsonMapper);
  }

  @Bean
  @ConditionalOnMissingBean
  public SnapshotDeserializer snapshotDeserializer(JsonMapper jsonMapper) {
    return new JacksonSnapshotDeserializer(jsonMapper);
  }
}
