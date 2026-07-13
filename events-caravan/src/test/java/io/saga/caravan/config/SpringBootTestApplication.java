package io.saga.caravan.config;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import tools.jackson.databind.json.JsonMapper;

@SpringBootApplication
@ComponentScan("io.saga.caravan")
public class SpringBootTestApplication {

  @Bean
  public JsonMapper jsonMapper() {
    return JsonMapper.builder().build();
  }
}
