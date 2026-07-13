package io.saga.caravan.event.sourcing.applying;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ApplyEvent {

  String value();
}
