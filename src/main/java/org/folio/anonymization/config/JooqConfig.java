package org.folio.anonymization.config;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.jooq.ConnectionProvider;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DataSourceConnectionProvider;
import org.springframework.boot.jooq.autoconfigure.DefaultConfigurationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceUtils;

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

  @Bean
  public ConnectionProvider connectionProvider(DataSource ds) {
    return new JooqConnectionProvider(ds);
  }

  public class JooqConnectionProvider extends DataSourceConnectionProvider {

    public JooqConnectionProvider(DataSource dataSource) {
      super(dataSource);
    }

    @Override
    public Connection acquire() {
      try {
        Connection conn = DataSourceUtils.getConnection(super.dataSource());
        // disables triggers/fk checks/etc for this session, speeding things up
        // and preventing some modules from attempting to update modification timestamps/etc
        conn.prepareCall("SET session_replication_role = replica").execute();
        return conn;
      } catch (SQLException e) {
        throw new DataAccessException("Something failed", e);
      }
    }
  }
}
