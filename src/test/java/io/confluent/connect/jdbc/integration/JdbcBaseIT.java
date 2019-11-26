/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.jdbc.integration;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.integration.mysql.MySqlSourceBaseIT;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.TableId;

import org.apache.kafka.common.config.AbstractConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public abstract class JdbcBaseIT {

  protected Map<String, String> properties;
  protected String databaseName;
  protected DatabaseDialect dialect;
  protected List<TableId> allTableIds;

  @Before
  public void beforeEach() {
    properties = new HashMap<>();
    dialect = createDialect();
  }

  @After
  public void afterEach() {
    if (dialect != null) {
      try {
        dialect.close();
      } finally {
        dialect = null;
      }
    }
  }

  protected void setAllTableIds(Collection<TableId> ids) {
    allTableIds = new ArrayList<>(ids);
  }

  protected void setAllTableIds(TableId...ids) {
    allTableIds = Arrays.asList(ids);
  }

  protected void setDatabase(String dbName) {
    databaseName = dbName;
  }

  protected void setConnectionUrl(String url) {
    set(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, url);
  }

  protected void setDialectName(String dialectName) {
    set(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG, dialectName);
  }

  protected void setUsername(String username) {
    set(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, username);
  }

  protected void setPassword(String password) {
    set(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, password);
  }

  protected abstract DatabaseDialect createDialect();

  protected void set(String name, Object value) {
    if (value == null) {
      properties.remove(name);
    } else {
      String str = null;
      if (value instanceof Object[]) {
        str = listOf((Object[]) value);
      } else if (value instanceof Duration) {
        Duration duration = (Duration) value;
        str = Long.toString(duration.toMillis());
      } else {
        str = value.toString();
      }
      properties.put(name, str);
    }
  }

  protected <T> String listOf(Object...values) {
    return listOf(Arrays.asList(values));
  }

  protected <T> String listOf(Collection<T> values) {
    return values.stream()
                 .map(Object::toString)
                 .collect(Collectors.joining(","));
  }
}
