/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.jdbc.integration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.integration.mysql.MySqlSourceBaseIT;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.util.TableId;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public abstract class JdbcSourceBaseIT extends JdbcBaseIT {

  protected JdbcSourceConnectorConfig config;
  protected JdbcSourceConnector connector;
  protected JdbcSourceTask task;
  protected List<SourceRecord> records;

  @Before
  @Override
  public void beforeEach() {
    super.beforeEach();
    setMaxBatchRows(1);
  }

  @After
  @Override
  public void afterEach() {
    try {
      stopConnector();
      stopTask();
      records = null;
    } finally {
      super.afterEach();
    }
  }

  @Test
  public void shouldCreateEmptyTaskConfigsListWithZeroMaxTasksAndNoQuery() {
    setQuery(null);
    startConnector();
    assertTaskConfigsWithQuery(0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateOneTaskConfigsListWithOneMaxTasksAndNoQuery() {
    setQuery(null);
    assertTaskConfigsWithoutQuery(1, allTableIds);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateEmptyTaskConfigsListWithZeroMaxTasksAndQuery() {
    setQuery("SELECT * FROM foo");
    startConnector();
    assertTaskConfigsWithQuery(0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateOneTaskConfigsListWithOneMaxTasksAndQuery() {
    setQuery("SELECT * FROM foo");
    startConnector();
    assertTaskConfigsWithoutQuery(1, allTableIds);
  }

  @Test
  public void shouldStartAndStopTaskWithoutCallingPoll() {
    startTask(emptyTable());
    stopTask();
  }

  @Test
  public void shouldStartAndStopTaskAfterCallingPoll() throws InterruptedException {
    startTask(emptyTable());
    records = task.poll();
    stopTask();
    assertTrue(task.isClosed());
  }

  @Test
  public void shouldReturnEmptyListOfRecordsAfterTaskIsStopped() throws InterruptedException {
    startTask(allTableIds);
    stopTask();
    assertTrue(task.isClosed());
    // When poll is called after stop, it should always return an empty list
    for (int i=0; i!=10; ++i) {
      assertNull(task.poll());
    }
  }

  @SuppressWarnings("varargs")
  protected void assertTaskConfigsWithoutQuery(int maxTasks, List<TableId>...expectedTaskTables) {
    List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);
    assertEquals(expectedTaskTables.length, taskConfigs.size());
    for (int i=0; i!=expectedTaskTables.length; ++i) {
      List<TableId> expectedTableIds = expectedTaskTables[i];
      Map<String, String> taskConfig = taskConfigs.get(i);
      Map<String, String> expected = new HashMap<>(properties);
      expected.put(JdbcSourceTaskConfig.TABLES_CONFIG, listOf(expectedTableIds));
      assertEquals(expected, taskConfig);
    }
  }

  protected void assertTaskConfigsWithQuery(int maxTasks) {
    String query = properties.get(JdbcSourceConnectorConfig.QUERY_CONFIG);
    assertNotNull(query);
    assertFalse(query.trim().isEmpty());

    List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);
    assertEquals(1, taskConfigs.size());
    Map<String, String> taskConfig = taskConfigs.get(0);
    Map<String, String> expected = new HashMap<>(properties);
    expected.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
    assertEquals(expected, taskConfig);
  }

  @Override
  protected DatabaseDialect createDialect() {
    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(properties);
    String url = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    return DatabaseDialects.findBestFor(url, config);
  }

  protected void startConnector() {
    config = new JdbcSourceConnectorConfig(properties);
    connector = new JdbcSourceConnector();
    connector.start(properties);
  }

  protected void stopConnector() {
    if (connector != null) {
      try {
        connector.stop();
      } finally {
        connector = null;
      }
    }
  }

  protected void startTask(Collection<TableId> tableIds) {
    startTask(listOf(tableIds));
  }

  protected void startTask(TableId...tableIds) {
    startTask(listOf((Object[]) tableIds));
  }

  protected void startTask(String tableIds) {
    task = new JdbcSourceTask();
    Map<String, String> taskConfig = new HashMap<>(properties);
    taskConfig.put(JdbcSourceTaskConfig.TABLES_CONFIG, tableIds);
    task.start(taskConfig);
  }

  protected void stopTask() {
    if (task != null) {
      try {
        task.stop();
      } finally {
        task = null;
      }
    }
  }

  protected abstract TableId emptyTable();

  protected void setMaxBatchRows(Integer maxBatchRows) {
    set(JdbcSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG, maxBatchRows);
  }

  protected void setMode(String mode) {
    set(JdbcSourceConnectorConfig.MODE_CONFIG, mode);
  }

  protected void setValidateNonNull(boolean validate) {
    set(JdbcSourceConnectorConfig.VALIDATE_NON_NULL_CONFIG, Boolean.toString(validate));
  }

  protected void setQuery(String query) {
    set(JdbcSourceConnectorConfig.QUERY_CONFIG, query);
  }

  protected void setModeToBulk() {
    set(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
  }

  protected void setModeToIncrementing(String incrementingColumn, boolean validateNonNull) {
    set(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
    set(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, incrementingColumn);
    setValidateNonNull(validateNonNull);
  }

  protected void setModeToTimestamp(String timestampColumn, boolean validateNonNull) {
    set(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_TIMESTAMP);
    set(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, timestampColumn);
    setValidateNonNull(validateNonNull);
  }

  protected void setModeToTimestampIncrementing(
      String incrementingColumn,
      String timestampColumn,
      boolean validateNonNull
  ) {
    set(
        JdbcSourceConnectorConfig.MODE_CONFIG,
        JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING
    );
    set(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, incrementingColumn);
    set(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, timestampColumn);
    setValidateNonNull(validateNonNull);
  }

  protected void setNumericMapping(JdbcSourceConnectorConfig.NumericMapping mapping) {
    set(JdbcSourceConnectorConfig.NUMERIC_MAPPING_CONFIG, mapping.name());
  }

  protected void setTopicPrefix(String prefix) {
    set(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, prefix);
  }

  protected void setTableWhitelist(String...tableNames) {
    set(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, tableNames);
  }

  protected void setTableBlacklist(String...tableNames) {
    set(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG, tableNames);
  }

  protected void setTableTypes(String...tableTypes) {
    set(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG, tableTypes);
  }

  protected void setPollInterval(Duration interval) {
    set(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, interval);
  }
}
