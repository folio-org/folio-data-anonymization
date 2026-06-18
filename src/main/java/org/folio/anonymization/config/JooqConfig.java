package org.folio.anonymization.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultDSLContext;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jooq.autoconfigure.DefaultConfigurationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

@Configuration
public class JooqConfig {

  // TODO: make this variable?
  private static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 10 * 60; // ten minutes

  @Bean
  public DefaultConfigurationCustomizer jooqDefaultConfigurationCustomizer() {
    // note: this is not actually handled by jOOQ; it seems that pgjdbc cancels eventually™
    // https://github.com/jOOQ/jOOQ/issues/14973#issuecomment-1518061328
    // https://stackoverflow.com/a/74521880/4236490
    return c -> c.settings().withQueryTimeout(DEFAULT_QUERY_TIMEOUT_SECONDS);
  }

  @Bean(name = "folioHikariConfig")
  @ConfigurationProperties("spring.datasource")
  public HikariConfig folioHikariConfig() {
    return new HikariConfig();
  }

  @Bean(name = "folioDataSource", destroyMethod = "close")
  @Primary
  public HikariDataSource folioDataSource(@Qualifier("folioHikariConfig") HikariConfig hikariConfig) {
    return new HikariDataSource(hikariConfig);
  }

  @Bean(name = "folioDslContext")
  @Primary
  public DSLContext folioDslContext(
    @Qualifier("folioDataSource") DataSource folioDataSource,
    ObjectProvider<DefaultConfigurationCustomizer> configurationCustomizers
  ) {
    DefaultConfiguration configuration = new DefaultConfiguration();
    configuration.set(new DataSourceConnectionProvider(new TransactionAwareDataSourceProxy(folioDataSource)));
    configuration.set(SQLDialect.POSTGRES);
    configurationCustomizers.orderedStream().forEach(customizer -> customizer.customize(configuration));
    return new DefaultDSLContext(configuration);
  }

  @Bean(name = "keycloakHikariConfig")
  @ConfigurationProperties("spring.keycloak-datasource")
  public HikariConfig keycloakHikariConfig() {
    return new HikariConfig();
  }

  @Bean(name = "keycloakDataSource", destroyMethod = "close")
  public HikariDataSource keycloakDataSource(@Qualifier("keycloakHikariConfig") HikariConfig hikariConfig) {
    return new HikariDataSource(hikariConfig);
  }

  @Bean(name = "keycloakDslContext")
  public DSLContext keycloakDslContext(
    @Qualifier("keycloakDataSource") DataSource keycloakDataSource,
    ObjectProvider<DefaultConfigurationCustomizer> configurationCustomizers
  ) {
    DefaultConfiguration configuration = new DefaultConfiguration();
    configuration.set(new DataSourceConnectionProvider(new TransactionAwareDataSourceProxy(keycloakDataSource)));
    configuration.set(SQLDialect.POSTGRES);
    configurationCustomizers.orderedStream().forEach(customizer -> customizer.customize(configuration));
    return new DefaultDSLContext(configuration);
  }
}
