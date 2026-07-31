package io.saga.caravan.autoconfigure;

import io.saga.caravan.event.EntityEventsRegistration;
import io.saga.caravan.event.EntityEventsRegistrationValidator;
import io.saga.caravan.event.EventPayloadClassMappingKeeper;
import io.saga.caravan.event.EventType;
import io.saga.caravan.event.consumer.EventConsumer;
import io.saga.caravan.event.consumer.EventMessageConsumer;
import io.saga.caravan.event.consumer.handler.HandlerBasedEventConsumer;
import io.saga.caravan.event.consumer.queue.SubscribedEntityQueueNamesKeeper;
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
import io.saga.caravan.messaging.MessageBatchDeletionProperties;
import io.saga.caravan.messaging.MessagingProperties;
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
      CaravanEventSourcingAutoConfiguration.class,
      CaravanEventMessagingAutoConfiguration.class);

  ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(autoConfigurations)
      .withUserConfiguration(ApplicationConfiguration.class)
      .withPropertyValues("caravan.event.messaging.queue-name-prefix=test-app");

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
    EntityEventsRegistration calculatorEvents() {
      return new EntityEventsRegistration() {

        @Override
        public String entityName() {
          return "calculator";
        }

        @Override
        public Map<String, Class<?>> eventToPayloadClass() {
          return Map.of("added", NumberPayload.class);
        }

        @Override
        public boolean isSubscriptionActive() {
          return true;
        }
      };
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationConfigurationWithoutJsonMapper {

    @Bean
    public EventProducer eventProducer() {
      return mock(EventProducer.class);
    }

    @Bean
    EntityEventsRegistration calculatorEvents() {
      return new EntityEventsRegistration() {

        @Override
        public String entityName() {
          return "calculator";
        }

        @Override
        public Map<String, Class<?>> eventToPayloadClass() {
          return Map.of("added", NumberPayload.class);
        }

        @Override
        public boolean isSubscriptionActive() {
          return true;
        }
      };
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
    EntityEventsRegistration calculatorEvents() {
      return new EntityEventsRegistration() {

        @Override
        public String entityName() {
          return "calculator";
        }

        @Override
        public Map<String, Class<?>> eventToPayloadClass() {
          return Map.of("added", NumberPayload.class);
        }

        @Override
        public boolean isSubscriptionActive() {
          return true;
        }
      };
    }

    @Bean
    EntityEventsRegistration calculatorEventsAgain() {
      return new EntityEventsRegistration() {

        @Override
        public String entityName() {
          return "calculator";
        }

        @Override
        public Map<String, Class<?>> eventToPayloadClass() {
          return Map.of("subtracted", NumberPayload.class);
        }

        @Override
        public boolean isSubscriptionActive() {
          return false;
        }
      };
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
    EntityEventsRegistration calculatorEvents() {
      return new EntityEventsRegistration() {

        @Override
        public String entityName() {
          return "calculator";
        }

        @Override
        public Map<String, Class<?>> eventToPayloadClass() {
          return Map.of("added", NumberPayload.class);
        }

        @Override
        public boolean isSubscriptionActive() {
          return true;
        }
      };
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
    EntityEventsRegistration calculatorEvents() {
      return new EntityEventsRegistration() {

        @Override
        public String entityName() {
          return "calculator";
        }

        @Override
        public Map<String, Class<?>> eventToPayloadClass() {
          return Map.of("added", NumberPayload.class);
        }

        @Override
        public boolean isSubscriptionActive() {
          return true;
        }
      };
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
              .getBean(EventPayloadClassMappingKeeper.class)
              .satisfies(map ->
                  assertThat(map.payloadClassFor(new EventType("calculator", "added")))
                      .hasValue(NumberPayload.class)));
    }

    @Test
    void shouldPreventDoubleRegistration() {
      new ApplicationContextRunner()
          .withConfiguration(autoConfigurations)
          .withUserConfiguration(ApplicationConfigurationWithDoubleRegistration.class)
          .withPropertyValues("caravan.event.messaging.queue-name-prefix=test-app")
          .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void applicationCanSupplyOwnRegistryComponents() {
      var ownEventPayloadClassMappingKeeper = mock(EventPayloadClassMappingKeeper.class);
      var ownValidator = mock(EntityEventsRegistrationValidator.class);

      contextRunner
          .withBean(EventPayloadClassMappingKeeper.class, () -> ownEventPayloadClassMappingKeeper)
          .withBean(EntityEventsRegistrationValidator.class, () -> ownValidator)
          .run(context -> {
            assertThat(context).getBean(EventPayloadClassMappingKeeper.class)
                .isSameAs(ownEventPayloadClassMappingKeeper);
            assertThat(context).getBean(EntityEventsRegistrationValidator.class)
                .isSameAs(ownValidator);
          });
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
          .withPropertyValues("caravan.event.messaging.queue-name-prefix=test-app")
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
          .withPropertyValues("caravan.event.messaging.queue-name-prefix=test-app")
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
          .withPropertyValues("caravan.event.messaging.queue-name-prefix=test-app")
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

  @Nested
  class Messaging {

    @Test
    void appliesDefaultsWhenNothingIsConfigured() {
      contextRunner.run(context -> {
        assertThat(context).getBean(MessagingProperties.class)
            .satisfies(properties -> {
              assertThat(properties.concurrency()).isEqualTo(10);
              assertThat(properties.maxPollSize()).isEqualTo(10);
              assertThat(properties.minPollSize()).isEqualTo(3);
              assertThat(properties.pollersCountCap()).isEqualTo(0);
              assertThat(properties.pollWaitSeconds()).isEqualTo(10);
              assertThat(properties.messageBatchDeletionProperties().maxDeleteBatchSize())
                  .isEqualTo(10);
              assertThat(properties.messageBatchDeletionProperties().periodSeconds())
                  .isEqualTo(1);
              assertThat(properties.messageBatchDeletionProperties().concurrency())
                  .isEqualTo(3);
            });
        assertThat(context).getBean(CaravanMessagingConfigurationProperties.class)
            .satisfies(properties -> assertThat(properties.gracefulShutdownSeconds()).isEqualTo(10));
      });
    }

    @Test
    void bindsConfiguredValues() {
      contextRunner
          .withPropertyValues(
              "caravan.event.messaging.concurrency=25",
              "caravan.event.messaging.max-poll-size=15",
              "caravan.event.messaging.min-poll-size=2",
              "caravan.event.messaging.pollers-count-cap=12",
              "caravan.event.messaging.poll-wait-seconds=7",
              "caravan.event.messaging.graceful-shutdown-seconds=17",
              "caravan.event.messaging.deletion.max-batch-size=7",
              "caravan.event.messaging.deletion.period-seconds=9",
              "caravan.event.messaging.deletion.concurrency=2"
          )
          .run(context -> {
            assertThat(context).getBean(MessagingProperties.class)
                .satisfies(properties -> {
                  assertThat(properties.concurrency()).isEqualTo(25);
                  assertThat(properties.maxPollSize()).isEqualTo(15);
                  assertThat(properties.minPollSize()).isEqualTo(2);
                  assertThat(properties.pollersCountCap()).isEqualTo(12);
                  assertThat(properties.pollWaitSeconds()).isEqualTo(7);
                  assertThat(properties.messageBatchDeletionProperties().maxDeleteBatchSize())
                      .isEqualTo(7);
                  assertThat(properties.messageBatchDeletionProperties().periodSeconds())
                      .isEqualTo(9);
                  assertThat(properties.messageBatchDeletionProperties().concurrency())
                      .isEqualTo(2);
                });
            assertThat(context).getBean(CaravanMessagingConfigurationProperties.class)
                .satisfies(properties -> assertThat(properties.gracefulShutdownSeconds()).isEqualTo(17));
          });
    }

    @Test
    void derivesQueueNamesOfSubscribedEntitiesFromThePrefix() {
      contextRunner
          .run(context ->
              assertThat(context).getBean(SubscribedEntityQueueNamesKeeper.class)
                  .satisfies(queueNames ->
                      assertThat(queueNames.queueNameOf("calculator"))
                          .contains("test-app_calculator")));
    }

    @Test
    void failsWhenQueueNamePrefixIsNotProvided() {
      new ApplicationContextRunner()
          .withConfiguration(autoConfigurations)
          .withUserConfiguration(ApplicationConfiguration.class)
          .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void applicationCanSupplyOwnMessagingComponents() {
      var ownMessagingProperties = MessagingProperties.builder()
          .concurrency(1)
          .maxPollSize(1)
          .minPollSize(1)
          .pollersCountCap(0)
          .pollWaitSeconds(1)
          .messageBatchDeletionProperties(
              MessageBatchDeletionProperties.builder()
                  .maxDeleteBatchSize(1)
                  .periodSeconds(1)
                  .concurrency(1)
                  .build())
          .build();
      var ownQueueNamesKeeper = mock(SubscribedEntityQueueNamesKeeper.class);

      contextRunner
          .withBean(MessagingProperties.class, () -> ownMessagingProperties)
          .withBean(SubscribedEntityQueueNamesKeeper.class, () -> ownQueueNamesKeeper)
          .run(context -> {
            assertThat(context).getBean(MessagingProperties.class).isSameAs(ownMessagingProperties);
            assertThat(context).getBean(SubscribedEntityQueueNamesKeeper.class).isSameAs(ownQueueNamesKeeper);
          });
    }
  }
}
