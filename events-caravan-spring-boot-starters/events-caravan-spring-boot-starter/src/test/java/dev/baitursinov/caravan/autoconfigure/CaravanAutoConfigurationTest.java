package dev.baitursinov.caravan.autoconfigure;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.EntityEventsRegistration;
import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.EventType;
import dev.baitursinov.caravan.event.consumer.EventConsumer;
import dev.baitursinov.caravan.event.consumer.EventMessageConsumer;
import dev.baitursinov.caravan.event.consumer.handler.HandlerBasedEventConsumer;
import dev.baitursinov.caravan.event.producer.EventProducer;
import dev.baitursinov.caravan.event.producer.EventProductionException;
import dev.baitursinov.caravan.event.serialization.EventDeserializer;
import dev.baitursinov.caravan.event.serialization.EventPayloadDeserializer;
import dev.baitursinov.caravan.event.serialization.EventPayloadSerializer;
import dev.baitursinov.caravan.event.serialization.EventSerializer;
import dev.baitursinov.caravan.event.sourcing.EventSourcingRepositoryContext;
import dev.baitursinov.caravan.event.sourcing.EventStore;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamRegistration;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamRegistry;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamWriter;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamWritingEventHandler;
import dev.baitursinov.caravan.event.sourcing.entity.stream.TimeBucket;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotDeserializer;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotSerializer;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotStore;
import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

@NullMarked
class CaravanAutoConfigurationTest {

  AutoConfigurations autoConfigurations = AutoConfigurations.of(
      CaravanEventRegistryAutoConfiguration.class,
      CaravanEventDrivenComponentsAutoConfiguration.class,
      CaravanJacksonSerializationAutoConfiguration.class,
      CaravanEventSourcingAutoConfiguration.class,
      CaravanEntityStreamAutoConfiguration.class);

  ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(autoConfigurations)
      .withUserConfiguration(ApplicationConfiguration.class);

  static class RecordingEventProducer implements EventProducer {

    final List<Event<?>> producedEvents = new ArrayList<>();

    @Override
    public void produce(Event<?> event) {
      producedEvents.add(event);
    }

    @Override
    public void produce(List<Event<?>> events) {
      producedEvents.addAll(events);
    }
  }

  interface CombinedEventStoreAndProducer extends EventStore, EventProducer {
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfiguration {

    RecordingEventProducer eventProducer = new RecordingEventProducer();

    @Bean
    public EventProducer eventProducer() {
      return eventProducer;
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
          "calculator", Map.of("added", NumberPayload.class));
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
          "calculator", Map.of("added", NumberPayload.class));
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
          "calculator", Map.of("added", NumberPayload.class));
    }

    @Bean
    EntityEventsRegistration calculatorEventsAgain() {
      return new EntityEventsRegistration(
          "calculator", Map.of("subtracted", NumberPayload.class));
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
          "calculator", Map.of("added", NumberPayload.class));
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
          "calculator", Map.of("added", NumberPayload.class));
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfigurationWithCombinedEventStoreAndProducer {

    @Bean
    public CombinedEventStoreAndProducer combinedEventStoreAndProducer() {
      return mock(CombinedEventStoreAndProducer.class);
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
          "calculator", Map.of("added", NumberPayload.class));
    }
  }

  record NumberPayload(int number) {
  }

  private static Event<Object> event(String eventName, Object payload) {
    return Event.builder()
        .entityReference(new EntityReference("calculator", "1"))
        .eventName(eventName)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(payload)
        .build();
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
    void shouldValidateEventsBeforeDelegatingToConfiguredProducer() {
      contextRunner.run(context -> {
        var applicationConfiguration = context.getBean(ApplicationConfiguration.class);
        var eventProducer = context.getBean(EventProducer.class);

        var validEvent = event("added", new NumberPayload(42));
        eventProducer.produce(validEvent);
        assertThat(applicationConfiguration.eventProducer.producedEvents).containsExactly(validEvent);

        var invalidEvent = event("removed", new NumberPayload(42));
        assertThatThrownBy(() -> eventProducer.produce(invalidEvent))
            .isInstanceOf(EventProductionException.class);
        assertThat(applicationConfiguration.eventProducer.producedEvents).containsExactly(validEvent);
      });
    }

    @Test
    void shouldFailIfNoEventProducerIsConfigured() {
      new ApplicationContextRunner()
          .withConfiguration(autoConfigurations)
          .withUserConfiguration(ApplicationConfigurationWithoutEventProducer.class)
          .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void shouldPreserveOtherRolesOfAProducerThatAlsoActsAsAnEventStore() {
      new ApplicationContextRunner()
          .withConfiguration(autoConfigurations)
          .withUserConfiguration(ApplicationConfigurationWithCombinedEventStoreAndProducer.class)
          .run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(EventStore.class);
            assertThat(context).hasSingleBean(EventSourcingRepositoryContext.class);
          });
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

  @Nested
  class EntityStream {

    @Test
    void doesNotConfigureStreamWritingHandlerWithoutAnEntityStreamWriter() {
      contextRunner.run(context ->
          assertThat(context).doesNotHaveBean(EntityStreamWritingEventHandler.class));
    }

    @Test
    void configuresStreamWritingHandlerWhenAnEntityStreamWriterIsPresent() {
      var entityStreamWriter = mock(EntityStreamWriter.class);

      contextRunner
          .withBean(EntityStreamWriter.class, () -> entityStreamWriter)
          .run(context -> assertThat(context).hasSingleBean(EntityStreamWritingEventHandler.class));
    }

    @Test
    void buildsEntityStreamRegistryFromRegistrationBeans() {
      var entityStreamWriter = mock(EntityStreamWriter.class);

      contextRunner
          .withBean(EntityStreamWriter.class, () -> entityStreamWriter)
          .withBean(EntityStreamRegistration.class,
              () -> new EntityStreamRegistration("calculator", TimeBucket.MONTHLY, 4))
          .run(context ->
              assertThat(context).getBean(EntityStreamRegistry.class)
                  .satisfies(registry ->
                      assertThat(registry.registrationFor("calculator")).isPresent()));
    }

    @Test
    void applicationCanSupplyOwnStreamWritingHandler() {
      var entityStreamWriter = mock(EntityStreamWriter.class);
      var ownHandler = new EntityStreamWritingEventHandler(
          mock(EntityStreamWriter.class), EntityStreamRegistry.createFor(List.of()));

      contextRunner
          .withBean(EntityStreamWriter.class, () -> entityStreamWriter)
          .withBean(EntityStreamWritingEventHandler.class, () -> ownHandler)
          .run(context ->
              assertThat(context).getBean(EntityStreamWritingEventHandler.class).isSameAs(ownHandler));
    }
  }
}
