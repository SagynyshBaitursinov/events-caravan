package dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Granularity at which the entity stream partitions entities by their creation time (the
 * timestamp of their first event), so that a stream partition's capacity is bounded by
 * creation rate rather than lifetime total. Rendered as a UTC ISO prefix of entity's
 * first-event timestamp, so a bucket is derivable from any sort key by
 * truncation. Must be immutable after first use.
 */
public enum TimeBucket {

  MINUTELY(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm")),
  HOURLY(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH")),
  DAILY(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
  MONTHLY(DateTimeFormatter.ofPattern("yyyy-MM"));

  private final DateTimeFormatter formatter;

  TimeBucket(DateTimeFormatter formatter) {
    this.formatter = formatter;
  }

  public String bucketOf(ZonedDateTime timestamp) {
    return formatter.format(timestamp);
  }
}
