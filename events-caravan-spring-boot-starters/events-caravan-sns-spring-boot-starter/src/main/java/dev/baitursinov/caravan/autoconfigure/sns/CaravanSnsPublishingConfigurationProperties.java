package dev.baitursinov.caravan.autoconfigure.sns;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configures the SNS-backed event message publishing autoconfigured under the {@value #PREFIX}
 * prefix: one topic per entity, named {@code ${topicNamePrefix}_${entityName}}.
 *
 * <p>Topic ARNs are looked up by name, so partition, region and account id are not configured.
 *
 * @param topicNamePrefix prefix prepended to each entity name to derive its topic name; required
 */
@ConfigurationProperties(CaravanSnsPublishingConfigurationProperties.PREFIX)
public record CaravanSnsPublishingConfigurationProperties(@Nullable String topicNamePrefix) {

  public static final String PREFIX = "caravan.event.publishing.sns";
}
