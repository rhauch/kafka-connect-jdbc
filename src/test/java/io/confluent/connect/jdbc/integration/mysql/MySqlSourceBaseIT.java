/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.jdbc.integration.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.integration.JdbcBaseIT;
import io.confluent.connect.jdbc.integration.JdbcSourceBaseIT;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.TableId;

import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.MySQLContainer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class MySqlSourceBaseIT extends JdbcSourceBaseIT {

  public interface AlmostEmptyDB {
    String DB_NAME = "almostEmptyDB";
    TableId EMPTY_TABLE = new TableId(null, DB_NAME, "emptyTable");
  }

  public interface TestSourceDB {
    String DB_NAME = "testSourceDB";
    TableId TYPES_TABLE = new TableId(null, DB_NAME, "types");
    TableId[] TABLE_IDS = {TYPES_TABLE};
    String[] TABLE_NAMES = {TYPES_TABLE.toString()};
  }

  public static final TableId[] ALL_TABLE_IDS = {
      AlmostEmptyDB.EMPTY_TABLE,
      TestSourceDB.TYPES_TABLE
  };

  /**
   * Return a reference to the container being used for the current test.
   *
   * @return the MySQL container; may not be null
   */
  protected abstract MySQLContainer<?> getContainer();

  @Before
  @Override
  public void beforeEach() {
    super.beforeEach();
    setDatabase(getContainer().getDatabaseName());
    setConnectionUrl(getContainer().getJdbcUrl());
    setUsername(getContainer().getUsername());
    setPassword(getContainer().getPassword());
    setModeToBulk();
    setTableWhitelist(TestSourceDB.TABLE_NAMES);
    setAllTableIds(ALL_TABLE_IDS);
  }

  @Test
  public void dialectShouldCreateConnection() throws SQLException {
    try (Connection conn = dialect.getConnection()) {
      assertTrue(
          "Expected 'types' table to exist",
          dialect.tableExists(conn, TestSourceDB.TYPES_TABLE)
      );
    }
  }

  @Test
  public void dialectShouldGetAllTables() throws SQLException {
    try (Connection conn = dialect.getConnection()) {
      List<TableId> tableIds = dialect.tableIds(conn);
      assertEquals(1, tableIds.size());
      assertTrue(tableIds.contains(TestSourceDB.TYPES_TABLE));
    }
  }

  @Test
  public void shouldLoadTypesTableWithQualifiedTableName() {
    setModeToBulk();
    setTableWhitelist(TestSourceDB.TYPES_TABLE.toString());
    startConnector();
  }

  @Test
  public void shouldLoadTypesTableWithBulk() {
    setModeToBulk();
    setTableWhitelist(TestSourceDB.TYPES_TABLE.toString());
  }

  @Test
  public void shouldLoadTypesTableWithIncrementing() {
  }

  @Override
  protected TableId emptyTable() {
    return AlmostEmptyDB.EMPTY_TABLE;
  }
}
