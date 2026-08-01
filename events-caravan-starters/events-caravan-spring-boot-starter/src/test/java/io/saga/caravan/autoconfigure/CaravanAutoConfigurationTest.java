package io.saga.caravan.autoconfigure;

import io.saga.caravan.event.EntityEventsRegistration;
import io.saga.caravan.event.EntityEventsRegistry;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.consumer.EventConsumer;
import io.saga.caravan.event.consumer.EventMessageConsumer;
import io.saga.caravan.event.consumer.handler.HandlerBasedEventConsumer;
import io.saga.caravan.event.producer.EventProducer;
import io.saga.caravan.event.producer.ValidatingEventProducer;
import io.saga.caravan.event.serialization.EventDeserializer;
import io.saga.caravan.event.serialization.EventPayloadDeserializer;
import io.saga.caravan.event.serialization.EventPayloadSerializer;
import io.saga.caravan.event.serialization.EventSerializer;
import io.saga.caravan.event.sourcing.EventSourcingRepositoryContext;
import io.saga.caravan.event.sourcing.EventStore;
import io.saga.caravan.event.sourcing.snapshot.SnapshotDeserializer;
import io.saga.caravan.event.sourcing.snapshot.SnapshotSerializer;
import io.saga.caravan.event.sourcing.snapshot.SnapshotStore;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@NullMarked
class CaravanAutoConfigurationTest {

  AutoConfigurations autoConfigurations = AutoConfigurations.of(
      CaravanEventRegistryAutoConfiguration.class,
      CaravanEventDrivenComponentsAutoConfiguration.class,
      CaravanJacksonSerializationAutoConfiguration.class,
      CaravanEventSourcingAutoConfiguration.class);

  ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(autoConfigurations)
      .withUserConfiguration(ApplicationConfiguration.class);

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfiguration {

    @Bean
    public EventProducer eventProducer() {
      return mock(EventProducer.class);
    }

    @Bean
    public EventStore eventStore() {
      return mock(EventStore.class);
    }

    @Bean
    public SnapshotStore snapshotStore() {
      return mock(SnapshotStore.class);
    }

    @Bean
    public JsonMapper jsonMapper() {
      return JsonMapper.builder().build();
    }

    @Bean
    EntityEventsRegistration calculatorEventsRegistration() {
      return new EntityEventsRegistration(
          "calculator", Map.of("added", NumberPayload.class), true);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfigurationWithoutJsonMapper {

    @Bean
    public EventProducer eventProducer() {
      return mock(EventProducer.class);
    }

    @Bean
    EntityEventsRegistration calculatorEventsRegistration() {
      return new EntityEventsRegistration(
          "calculator", Map.of("added", NumberPayload.class), true);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfigurationWithDoubleRegistration {

    @Bean
    public EventProducer eventProducer() {
      return mock(EventProducer.class);
    }

    @Bean
    public JsonMapper jsonMapper() {
      return JsonMapper.builder().build();
    }

    @Bean
    EntityEventsRegistration calculatorEventsRegistration() {
      return new EntityEventsRegistration(
          "calculator", Map.of("added", NumberPayload.class), true);
    }

    @Bean
    EntityEventsRegistration calculatorEventsAgain() {
      return new EntityEventsRegistration(
          "calculator", Map.of("subtracted", NumberPayload.class), false);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfigurationWithoutStorageBackend {

    @Bean
    public EventProducer eventProducer() {
      return mock(EventProducer.class);
    }

    @Bean
    public JsonMapper jsonMapper() {
      return JsonMapper.builder().build();
    }

    @Bean
    EntityEventsRegistration calculatorEventsRegistration() {
      return new EntityEventsRegistration(
          "calculator", Map.of("added", NumberPayload.class), true);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfigurationWithoutEventProducer {

    @Bean
    public EventStore eventStore() {
      return mock(EventStore.class);
    }

    @Bean
    public SnapshotStore snapshotStore() {
      return mock(SnapshotStore.class);
    }

    @Bean
    public JsonMapper jsonMapper() {
      return JsonMapper.builder().build();
    }

    @Bean
    EntityEventsRegistration calculatorEventsRegistration() {
      return new EntityEventsRegistration(
          "calculator", Map.of("added", NumberPayload.class), true);
    }
  }

  record NumberPayload(int number) {
  }

  @Nested
  class Registry {

    @Test
    void indexesEveryRegisteredEventPayloadClass() {
      contextRunner.run(context ->
          assertThat(context)
              .getBean(EntityEventsRegistry.class)
              .satisfies(map ->
                  assertThat(map.payloadClassFor(new EventType("calculator", "added")))
                      .hasValue(NumberPayload.class)));
    }

    @Test
    void shouldPreventDoubleRegistration() {
      new ApplicationContextRunner()
          .withConfiguration(autoConfigurations)
          .withUserConfiguration(ApplicationConfigurationWithDoubleRegistration.class)
          .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void applicationCanSupplyOwnEntityEventsRegistry() {
      var ownEntityEventsRegistry = mock(EntityEventsRegistry.class);

      contextRunner
          .withBean(EntityEventsRegistry.class, () -> ownEntityEventsRegistry)
          .run(context ->
              assertThat(context).getBean(EntityEventsRegistry.class)
                  .isSameAs(ownEntityEventsRegistry));
    }
  }

  @Nested
  class Serialization {

    @Test
    void applicationCanSupplyOwnSerializationComponents() {
      var ownDeserializer = mock(EventDeserializer.class);
      var ownSerializer = mock(EventSerializer.class);
      var ownPayloadDeserializer = mock(EventPayloadDeserializer.class);
      var ownPayloadSerializer = mock(EventPayloadSerializer.class);
      var ownSnapshotSerializer = mock(SnapshotSerializer.class);
      var ownSnapshotDeserializer = mock(SnapshotDeserializer.class);

      contextRunner
          .withClassLoader(new FilteredClassLoader(JsonMapper.class))
          .withBean(EventDeserializer.class, () -> ownDeserializer)
          .withBean(EventSerializer.class, () -> ownSerializer)
          .withBean(EventPayloadDeserializer.class, () -> ownPayloadDeserializer)
          .withBean(EventPayloadSerializer.class, () -> ownPayloadSerializer)
          .withBean(SnapshotSerializer.class, () -> ownSnapshotSerializer)
          .withBean(SnapshotDeserializer.class, () -> ownSnapshotDeserializer)
          .run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(CaravanJacksonSerializationAutoConfiguration.class);

            assertThat(context).getBean(EventDeserializer.class).isSameAs(ownDeserializer);
            assertThat(context).getBean(EventSerializer.class).isSameAs(ownSerializer);
            assertThat(context).getBean(EventPayloadDeserializer.class).isSameAs(ownPayloadDeserializer);
            assertThat(context).getBean(EventPayloadSerializer.class).isSameAs(ownPayloadSerializer);
            assertThat(context).getBean(SnapshotSerializer.class).isSameAs(ownSnapshotSerializer);
            assertThat(context).getBean(SnapshotDeserializer.class).isSameAs(ownSnapshotDeserializer);
          });
    }

    @Test
    void failsIfNoJsonMapperIsProvided() {
      new ApplicationContextRunner()
          .withConfiguration(autoConfigurations)
          .withUserConfiguration(ApplicationConfigurationWithoutJsonMapper.class)
          .run(context -> assertThat(context).hasFailed());
    }
  }

  @Nested
  class EventSourcing {

    @Test
    void setsUpRepositoryContextWhenAStorageBackendIsProvided() {
      contextRunner.run(context ->
          assertThat(context).hasSingleBean(EventSourcingRepositoryContext.class));
    }

    @Test
    void failsWithoutEventStorageBackend() {
      new ApplicationContextRunner()
          .withConfiguration(autoConfigurations)
          .withUserConfiguration(ApplicationConfigurationWithoutStorageBackend.class)
          .run(context -> assertThat(context).hasFailed());
    }
  }

  @Nested
  class EventDriven {

    @Test
    void shouldSetupEventConsumer() {
      contextRunner.run(context -> {
        assertThat(context).hasSingleBean(EventMessageConsumer.class);
        assertThat(context).getBean(EventConsumer.class).isInstanceOf(HandlerBasedEventConsumer.class);
      });
    }

    @Test
    void shouldWrapEventProducerIntoValidatingWrapper() {
      contextRunner.run(context ->
          assertThat(context).getBean(EventProducer.class).isInstanceOf(ValidatingEventProducer.class));
    }

    @Test
    void shouldFailIfNoEventProducerIsConfigured() {
      new ApplicationContextRunner()
          .withConfiguration(autoConfigurations)
          .withUserConfiguration(ApplicationConfigurationWithoutEventProducer.class)
          .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void applicationCanSupplyOwnEventConsumerComponents() {
      var ownEventConsumer = mock(EventConsumer.class);
      var ownEventMessageConsumer = mock(EventMessageConsumer.class);

      contextRunner
          .withBean(EventConsumer.class, () -> ownEventConsumer)
          .withBean(EventMessageConsumer.class, () -> ownEventMessageConsumer)
          .run(context -> {
            assertThat(context).getBean(EventConsumer.class).isSameAs(ownEventConsumer);
            assertThat(context).getBean(EventMessageConsumer.class).isSameAs(ownEventMessageConsumer);
          });
    }
  }
}
