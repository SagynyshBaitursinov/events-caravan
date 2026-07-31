package io.saga.caravan.utils;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TextUtils {

  public static boolean hasText(@Nullable String text) {
    return text != null && !text.isBlank();
  }
}
