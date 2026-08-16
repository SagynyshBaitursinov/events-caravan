package dev.baitursinov.caravan.event.sourcing.entity.stream;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.nio.charset.StandardCharsets;

/**
 * FNV-1a, 64-bit variant, over UTF-8 bytes. Used by {@link EntityStreamWritingEventHandler} to
 * shard entities within a time bucket. Specified here (rather than relying on
 * {@code String.hashCode()}) so any other language or tool computes the same shard for a given
 * {@code entityId} — see the design's determinism invariant.
 *
 * @see <a href="http://www.isthe.com/chongo/tech/comp/fnv/index.html">FNV hash reference</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Fnv1a64 {

  private static final long OFFSET_BASIS = 0xcbf29ce484222325L;
  private static final long PRIME = 0x100000001b3L;

  static long hash(String value) {
    long hash = OFFSET_BASIS;
    for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
      hash ^= (b & 0xff);
      hash *= PRIME;
    }
    return hash;
  }
}
