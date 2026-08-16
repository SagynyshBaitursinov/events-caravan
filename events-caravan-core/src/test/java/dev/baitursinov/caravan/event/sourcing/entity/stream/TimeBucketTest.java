package dev.baitursinov.caravan.event.sourcing.entity.stream;

import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class TimeBucketTest {

  ZonedDateTime testTimestamp = ZonedDateTime.parse("2026-08-10T14:03:22.123Z");

  @Test
  void yearlyTruncatesToYearAndMonth() {
    assertThat(TimeBucket.YEARLY.locationOf(testTimestamp)).isEqualTo("2026");
  }

  @Test
  void monthlyTruncatesToYearAndMonth() {
    assertThat(TimeBucket.MONTHLY.locationOf(testTimestamp)).isEqualTo("2026-08");
  }

  @Test
  void dailyTruncatesToCalendarDay() {
    assertThat(TimeBucket.DAILY.locationOf(testTimestamp)).isEqualTo("2026-08-10");
  }

  @Test
  void hourlyTruncatesToHour() {
    assertThat(TimeBucket.HOURLY.locationOf(testTimestamp)).isEqualTo("2026-08-10T14");
  }

  @Test
  void minutelyTruncatesToMinute() {
    assertThat(TimeBucket.MINUTELY.locationOf(testTimestamp)).isEqualTo("2026-08-10T14:03");
  }

  @Test
  void secondlyTruncatesToSecond() {
    assertThat(TimeBucket.SECONDLY.locationOf(testTimestamp)).isEqualTo("2026-08-10T14:03:22");

  }
}
