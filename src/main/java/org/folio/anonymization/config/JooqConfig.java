package org.folio.anonymization.config;

import org.springframework.boot.jooq.autoconfigure.DefaultConfigurationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JooqConfig {

  // TODO: make this variable?
  private static final int QUERY_TIMEOUT_SECONDS = 10 * 60; // ten minutes

  @Bean
  public DefaultConfigurationCustomizer jooqDefaultConfigurationCustomizer() {
    // note: this is not actually handled by jOOQ; it seems that pgjdbc cancels eventually™
    // https://github.com/jOOQ/jOOQ/issues/14973#issuecomment-1518061328
    // https://stackoverflow.com/a/74521880/4236490
    return c -> c.settings().withQueryTimeout(QUERY_TIMEOUT_SECONDS);
  }
}
