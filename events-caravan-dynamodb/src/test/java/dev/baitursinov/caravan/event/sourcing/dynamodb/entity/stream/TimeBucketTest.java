package dev.baitursinov.caravan.event.sourcing.dynamodb.entity.stream;

import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class TimeBucketTest {

  ZonedDateTime testTimestamp = ZonedDateTime.parse("2026-08-10T14:03:22.123Z");

  @Test
  void monthlyTruncatesToYearAndMonth() {
    assertThat(TimeBucket.MONTHLY.bucketOf(testTimestamp)).isEqualTo("2026-08");
  }

  @Test
  void dailyTruncatesToCalendarDay() {
    assertThat(TimeBucket.DAILY.bucketOf(testTimestamp)).isEqualTo("2026-08-10");
  }

  @Test
  void hourlyTruncatesToHour() {
    assertThat(TimeBucket.HOURLY.bucketOf(testTimestamp)).isEqualTo("2026-08-10T14");
  }

  @Test
  void minutelyTruncatesToMinute() {
    assertThat(TimeBucket.MINUTELY.bucketOf(testTimestamp)).isEqualTo("2026-08-10T14:03");
  }
}
