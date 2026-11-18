package dev.baitursinov.caravan.autoconfigure;

import dev.baitursinov.caravan.entity.EntityReference;
import dev.baitursinov.caravan.event.EntityEventsRegistration;
import dev.baitursinov.caravan.event.EntityEventsRegistry;
import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.EventType;
import dev.baitursinov.caravan.event.consumer.EventConsumer;
import dev.baitursinov.caravan.event.consumer.EventMessageConsumer;
import dev.baitursinov.caravan.event.consumer.handler.HandlerBasedEventConsumer;
import dev.baitursinov.caravan.event.producer.EventProductionException;
import dev.baitursinov.caravan.event.producer.SequencedEventProducer;
import dev.baitursinov.caravan.event.producer.SequencedEventWriter;
import dev.baitursinov.caravan.event.producer.UnsequencedEventProducer;
import dev.baitursinov.caravan.event.producer.UnsequencedEventWriter;
import dev.baitursinov.caravan.event.producer.ValidatedEvents;
import dev.baitursinov.caravan.event.publisher.EventMessage;
import dev.baitursinov.caravan.event.publisher.EventMessagePublisher;
import dev.baitursinov.caravan.event.serialization.EventDeserializer;
import dev.baitursinov.caravan.event.serialization.EventPayloadDeserializer;
import dev.baitursinov.caravan.event.serialization.EventPayloadSerializer;
import dev.baitursinov.caravan.event.serialization.EventSerializer;
import dev.baitursinov.caravan.event.sourcing.EventSourcedEntitySetupException;
import dev.baitursinov.caravan.event.sourcing.EventSourcingRepositoryContext;
import dev.baitursinov.caravan.event.sourcing.EventStore;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamRegistration;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamRegistrationException;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamRegistry;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamWriter;
import dev.baitursinov.caravan.event.sourcing.entity.stream.EntityStreamWritingEventHandler;
import dev.baitursinov.caravan.event.sourcing.entity.stream.TimeBucket;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotDeserializer;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotSerializer;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotStore;
import dev.baitursinov.caravan.event.sourcing.snapshot.SnapshotTaker;
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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@NullMarked
class CaravanAutoConfigurationTest {

  AutoConfigurations autoConfigurations = AutoConfigurations.of(
      CaravanEventRegistryAutoConfiguration.class,
      CaravanEventDrivenComponentsAutoConfiguration.class,
      CaravanEventProducersAutoConfiguration.class,
      CaravanMessagePublishingAutoConfiguration.class,
      CaravanJacksonSerializationAutoConfiguration.class,
      CaravanEventSourcingAutoConfiguration.class,
      CaravanEntityStreamAutoConfiguration.class);

  ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(autoConfigurations)
      .withUserConfiguration(ApplicationConfiguration.class);

  static class RecordingSequencedEventWriter implements SequencedEventWriter {

    final List<Event<?>> writtenEvents = new ArrayList<>();

    @Override
    public void write(ValidatedEvents events) {
      writtenEvents.addAll(events.events());
    }
  }

  static class RecordingUnsequencedEventWriter implements UnsequencedEventWriter {

    final List<Event<?>> writtenEvents = new ArrayList<>();

    @Override
    public void write(ValidatedEvents events) {
      writtenEvents.addAll(events.events());
    }
  }

  interface CombinedEventStoreAndWriter extends EventStore, SequencedEventWriter {
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfiguration {

    RecordingSequencedEventWriter eventWriter = new RecordingSequencedEventWriter();

    @Bean
    public SequencedEventWriter eventWriter() {
      return eventWriter;
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
    public SequencedEventWriter eventWriter() {
      return mock(SequencedEventWriter.class);
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
    public SequencedEventWriter eventWriter() {
      return mock(SequencedEventWriter.class);
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
    public SequencedEventWriter eventWriter() {
      return mock(SequencedEventWriter.class);
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
  static class ApplicationConfigurationWithoutEventWriter {

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
  static class ApplicationConfigurationWithCombinedEventStoreAndWriter {

    @Bean
    public CombinedEventStoreAndWriter combinedEventStoreAndWriter() {
      return mock(CombinedEventStoreAndWriter.class);
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
    return Event.sequenced()
        .entityReference(new EntityReference("calculator", "1"))
        .eventName(eventName)
        .sequenceNumber(1)
        .timestamp(ZonedDateTime.now())
        .payload(payload)
        .build();
  }

  private static Event<Object> unsequencedEvent(String eventName, Object payload) {
    return Event.unsequenced()
        .entityReference(new EntityReference("calculator", "1"))
        .eventName(eventName)
        .naturalKey(UUID.randomUUID().toString())
        .timestamp(ZonedDateTime.now())
        .payload(payload)
        .build();
  }

  private static EventSerializer serializerGiving(String body) {
    var eventSerializer = mock(EventSerializer.class);
    when(eventSerializer.serialize(any(Event.class))).thenReturn(body);
    return eventSerializer;
  }

  private static EventMessagePublisher recordingPublisher(List<EventMessage> publishedMessages) {
    return new EventMessagePublisher() {

      @Override
      public void publish(EventMessage eventMessage) {
        publishedMessages.add(eventMessage);
      }

      @Override
      public void publish(List<EventMessage> eventMessages) {
        publishedMessages.addAll(eventMessages);
      }
    };
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

    @Test
    void setsUpRepositoryContextWithoutSnapshotStoreWhenNoSnapshotTakersAreDeclared() {
      new ApplicationContextRunner()
          .withConfiguration(autoConfigurations)
          .withUserConfiguration(ApplicationConfigurationWithoutStorageBackend.class)
          .withBean(EventStore.class, () -> mock(EventStore.class))
          .run(context -> {
            assertThat(context).doesNotHaveBean(SnapshotStore.class);
            assertThat(context).hasSingleBean(EventSourcingRepositoryContext.class);
          });
    }

    @Test
    void failsWithSnapshotTakersButWithoutSnapshotStore() {
      new ApplicationContextRunner()
          .withConfiguration(autoConfigurations)
          .withUserConfiguration(ApplicationConfigurationWithoutStorageBackend.class)
          .withBean(EventStore.class, () -> mock(EventStore.class))
          .withBean(SnapshotTaker.class, () -> mock(SnapshotTaker.class))
          .run(context -> assertThat(context).getFailure()
              .rootCause()
              .isInstanceOf(EventSourcedEntitySetupException.class)
              .hasMessageContaining("no SnapshotStore is configured"));
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
    void shouldValidateEventsBeforeHandingThemToConfiguredWriter() {
      contextRunner.run(context -> {
        var applicationConfiguration = context.getBean(ApplicationConfiguration.class);
        var eventProducer = context.getBean(SequencedEventProducer.class);

        var validEvent = event("added", new NumberPayload(42));
        eventProducer.produce(validEvent);
        assertThat(applicationConfiguration.eventWriter.writtenEvents).containsExactly(validEvent);

        var invalidEvent = event("removed", new NumberPayload(42));
        assertThatThrownBy(() -> eventProducer.produce(invalidEvent))
            .isInstanceOf(EventProductionException.class);
        assertThat(applicationConfiguration.eventWriter.writtenEvents).containsExactly(validEvent);
      });
    }

    @Test
    void shouldNotConfigureUnsequencedEventProducerWithoutAnEventMessagePublisher() {
      contextRunner.run(context -> assertThat(context).doesNotHaveBean(UnsequencedEventProducer.class));
    }

    @Test
    void shouldPublishUnsequencedEventsWithoutHandingThemToSequencedEventWriter() {
      var publishedMessages = new ArrayList<EventMessage>();

      contextRunner
          .withBean(EventMessagePublisher.class, () -> recordingPublisher(publishedMessages))
          .withBean(EventSerializer.class, () -> serializerGiving("serialized event"))
          .run(context -> {
            assertThat(context).hasSingleBean(UnsequencedEventProducer.class);
            assertThat(context).hasSingleBean(SequencedEventProducer.class);
            var applicationConfiguration = context.getBean(ApplicationConfiguration.class);
            var unsequencedEventProducer = context.getBean(UnsequencedEventProducer.class);

            var unsequencedEvent = unsequencedEvent("added", new NumberPayload(42));
            unsequencedEventProducer.produce(unsequencedEvent);
            unsequencedEventProducer.produce(List.of(unsequencedEvent));

            assertThat(applicationConfiguration.eventWriter.writtenEvents).isEmpty();
            assertThat(publishedMessages)
                .hasSize(2)
                .containsOnly(new EventMessage(unsequencedEvent.eventReference(), "serialized event"));

            var event = event("added", new NumberPayload(42));
            context.getBean(SequencedEventProducer.class).produce(event);

            assertThat(applicationConfiguration.eventWriter.writtenEvents).containsExactly(event);
            assertThat(publishedMessages).hasSize(2);
          });
    }

    @Test
    void shouldValidateUnsequencedEventsBeforePublishingThem() {
      var publishedMessages = new ArrayList<EventMessage>();

      contextRunner
          .withBean(EventMessagePublisher.class, () -> recordingPublisher(publishedMessages))
          .run(context -> {
            var unsequencedEventProducer = context.getBean(UnsequencedEventProducer.class);

            assertThatThrownBy(() -> unsequencedEventProducer.produce(unsequencedEvent("removed", new NumberPayload(42))))
                .isInstanceOf(EventProductionException.class)
                .hasMessageContaining("no registered payload class");
            assertThat(publishedMessages).isEmpty();
          });
    }

    @Test
    void applicationCanSupplyOwnUnsequencedEventWriter() {
      var publishedMessages = new ArrayList<EventMessage>();
      var ownUnsequencedEventWriter = new RecordingUnsequencedEventWriter();

      contextRunner
          .withBean(EventMessagePublisher.class, () -> recordingPublisher(publishedMessages))
          .withBean(UnsequencedEventWriter.class, () -> ownUnsequencedEventWriter)
          .run(context -> {
            assertThat(context).hasSingleBean(UnsequencedEventWriter.class);
            var unsequencedEventProducer = context.getBean(UnsequencedEventProducer.class);
            var event = unsequencedEvent("added", new NumberPayload(42));

            unsequencedEventProducer.produce(event);
            assertThatThrownBy(() -> unsequencedEventProducer.produce(unsequencedEvent("removed", new NumberPayload(42))))
                .isInstanceOf(EventProductionException.class);

            assertThat(ownUnsequencedEventWriter.writtenEvents).containsExactly(event);
            assertThat(publishedMessages).isEmpty();
            assertThat(context.getBean(ApplicationConfiguration.class).eventWriter.writtenEvents).isEmpty();
          });
    }

    @Test
    void shouldFailToPublishUnsequencedEventsWithoutAnEventSerializer() {
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(
              CaravanEventRegistryAutoConfiguration.class,
              CaravanMessagePublishingAutoConfiguration.class,
              CaravanEventProducersAutoConfiguration.class))
          .withBean(EventMessagePublisher.class, () -> recordingPublisher(new ArrayList<>()))
          .run(context -> assertThat(context)
              .hasFailed()
              .getFailure()
              .rootCause()
              .hasMessageContaining(EventSerializer.class.getName()));
    }

    @Test
    void shouldFailIfNoSequencedEventWriterIsConfigured() {
      new ApplicationContextRunner()
          .withConfiguration(autoConfigurations)
          .withUserConfiguration(ApplicationConfigurationWithoutEventWriter.class)
          .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void shouldFailWithTwoSequencedEventWritersEvenIfOneMatchesTheParameterName() {
      new ApplicationContextRunner()
          .withConfiguration(autoConfigurations)
          .withUserConfiguration(ApplicationConfigurationWithoutStorageBackend.class)
          .withBean("sequencedEventWriter", SequencedEventWriter.class, () -> mock(SequencedEventWriter.class))
          .run(context -> assertThat(context).getFailure()
              .rootCause()
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("Exactly one SequencedEventWriter"));
    }

    @Test
    void shouldUseOneBeanAsBothEventStoreAndSequencedEventWriter() {
      new ApplicationContextRunner()
          .withConfiguration(autoConfigurations)
          .withUserConfiguration(ApplicationConfigurationWithCombinedEventStoreAndWriter.class)
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
    void failsWithEntityStreamRegistrationsButWithoutAnEntityStreamWriter() {
      contextRunner
          .withBean(EntityStreamRegistration.class,
              () -> new EntityStreamRegistration("calculator", TimeBucket.MONTHLY, 4))
          .run(context -> assertThat(context).getFailure()
              .rootCause()
              .isInstanceOf(EntityStreamRegistrationException.class)
              .hasMessageContaining("no EntityStreamWriter is configured")
              .hasMessageContaining("calculator"));
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
