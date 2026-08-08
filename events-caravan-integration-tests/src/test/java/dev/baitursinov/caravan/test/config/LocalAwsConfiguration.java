package dev.baitursinov.caravan.test.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;

import static software.amazon.awssdk.regions.Region.US_EAST_1;

@Configuration
public class LocalAwsConfiguration {

  private static final URI LOCAL_ENDPOINT = URI.create("http://localhost:4566");

  @Bean
  public AwsCredentialsProvider awsCredentialsProvider() {
    return StaticCredentialsProvider.create(
        AwsBasicCredentials.create("fake", "fake"));
  }

  @Bean
  public DynamoDbClient dynamoDbClient(AwsCredentialsProvider awsCredentialsProvider) {
    return DynamoDbClient.builder()
        .credentialsProvider(awsCredentialsProvider)
        .region(US_EAST_1)
        .endpointOverride(LOCAL_ENDPOINT)
        .build();
  }

  @Bean
  public SqsClient sqsClient(AwsCredentialsProvider awsCredentialsProvider) {
    return SqsClient.builder()
        .credentialsProvider(awsCredentialsProvider)
        .region(US_EAST_1)
        .endpointOverride(LOCAL_ENDPOINT)
        .build();
  }
}
