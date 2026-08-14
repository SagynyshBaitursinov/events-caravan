package dev.baitursinov.caravan.event.consumer.handler;

import dev.baitursinov.caravan.event.Event;
import dev.baitursinov.caravan.event.consumer.EventConsumer;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Optional;

/**
 * An {@link EventConsumer} that dispatches each event to every {@link EventHandler} whose
 * declared payload type matches the event's payload, in the order the handlers were provided.
 * The payload type each handler accepts is determined once per handler class.
 */
@Slf4j
public class HandlerBasedEventConsumer implements EventConsumer {

  private static final String HANDLE_METHOD_NAME = "handle";

  private static final ClassValue<Optional<Class<?>>> HANDLED_PAYLOAD_CLASSES = new ClassValue<>() {
    @Override
    protected Optional<Class<?>> computeValue(Class<?> handlerClass) {
      return retrieveEventPayloadParameter(handlerClass);
    }
  };

  private final Collection<EventHandler<?>> eventHandlers;

  public HandlerBasedEventConsumer(Collection<EventHandler<?>> eventHandlers) {
    this.eventHandlers = eventHandlers;
    eventHandlers.forEach(handler -> HANDLED_PAYLOAD_CLASSES.get(handler.getClass()));
  }

  @Override
  public void consume(Event<?> event) {
    log.debug("Received {} for consumption", event.eventReference());

    eventHandlers.stream()
        .filter(handler ->
            hasAssignablePayloadClass(handler, event))
        .forEach(handler ->
            consumeTypeCastingIfInterested(handler, event));
  }

  private boolean hasAssignablePayloadClass(EventHandler<?> eventHandler,
                                            Event<?> event) {
    var hasAssignablePayloadClass = HANDLED_PAYLOAD_CLASSES.get(eventHandler.getClass())
        .map(payloadClass ->
            payloadClass.isAssignableFrom(event.payload().getClass()))
        .orElse(false);

    log.debug("{} does not have a payload class assignable from {}",
        eventHandler.getClass().getSimpleName(),
        event);

    return hasAssignablePayloadClass;
  }

  private static Optional<Class<?>> retrieveEventPayloadParameter(Class<?> handlerClass) {
    try {
      Method handleMethod = handlerClass.getMethod(HANDLE_METHOD_NAME, Event.class);
      Type[] parameterTypes = handleMethod.getGenericParameterTypes();
      if (parameterTypes.length == 1 && parameterTypes[0] instanceof ParameterizedType parameterizedType) {
        Type typeArgument = parameterizedType.getActualTypeArguments()[0];
        if (typeArgument instanceof Class<?> result) {
          return Optional.of(result);
        } else {
          return Optional.empty();
        }
      } else {
        throw new EventHandlerSetupException("Cannot extract handle method parameter type");
      }
    } catch (NoSuchMethodException exception) {
      throw new EventHandlerSetupException("Cannot extract handle method parameter type", exception);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> void consumeTypeCastingIfInterested(EventHandler<? extends T> eventHandler,
                                                  Event<? extends T> event) {
    var typeCastedEvent = (Event<T>) event;
    var typeCastedEventHandler = (EventHandler<T>) eventHandler;

    if (typeCastedEventHandler.isOfInterest(typeCastedEvent)) {
      log.debug("Handling {} with {}", event, eventHandler.getClass().getSimpleName());
      typeCastedEventHandler.handle(typeCastedEvent);
    } else {
      log.debug("{} reported no interest in {}", eventHandler.getClass().getSimpleName(), event);
    }
  }
}
