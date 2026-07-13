package io.saga.caravan.event.consumer.handler;

import io.saga.caravan.event.Event;
import io.saga.caravan.event.consumer.EventConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class HandlerBasedEventConsumer implements EventConsumer {

  private static final String HANDLE_METHOD_NAME = "handle";

  private final Collection<EventHandler<?>> eventHandlers;
  private final Map<Class<?>, Class<?>> eventHandlerPayloadClassCache = new ConcurrentHashMap<>();

  @Override
  public void consume(Event<?> event) {
    eventHandlers.stream()
        .filter(handler ->
            hasInterestingPayloadClass(handler, event))
        .forEach(handler ->
            consumeTypeCastingIfInterested(handler, event));
  }

  private boolean hasInterestingPayloadClass(EventHandler<?> eventHandler,
                                             Event<?> event) {
    var payloadClass = extractPayloadClass(eventHandler);
    if (payloadClass == null) {
      return false;
    }

    return payloadClass.isAssignableFrom(event.payload().getClass());
  }

  @Nullable
  private Class<?> extractPayloadClass(EventHandler<?> eventHandler) {
    return eventHandlerPayloadClassCache.computeIfAbsent(
        eventHandler.getClass(),
        this::retrieveEventPayloadParameter);
  }

  @Nullable
  private Class<?> retrieveEventPayloadParameter(Class<?> handlerClass) {
    try {
      Method handleMethod = handlerClass.getMethod(HANDLE_METHOD_NAME, Event.class);
      Type[] parameterTypes = handleMethod.getGenericParameterTypes();
      if (parameterTypes.length == 1 && parameterTypes[0] instanceof ParameterizedType parameterizedType) {
        Type typeArgument = parameterizedType.getActualTypeArguments()[0];
        if (typeArgument instanceof Class<?> result) {
          return result;
        } else {
          return null;
        }
      } else {
        throw new IllegalStateException("Cannot extract handle method parameter type");
      }
    } catch (NoSuchMethodException exception) {
      throw new IllegalStateException("Cannot extract handle method parameter type", exception);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> void consumeTypeCastingIfInterested(EventHandler<? extends T> eventHandler,
                                                  Event<? extends T> event) {
    var typeCastedEvent = (Event<T>) event;
    var typeCastedEventHandler = (EventHandler<T>) eventHandler;

    if (typeCastedEventHandler.isOfInterest(typeCastedEvent)) {
      typeCastedEventHandler.handle(typeCastedEvent);
    }
  }
}
