/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.jdbc.integration.mysql;

import org.junit.ClassRule;
import org.testcontainers.containers.MySQLContainer;

public class MySqlSource_5_6_IT extends MySqlSourceBaseIT {

  @ClassRule
  protected static final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:5.6")
      .withConfigurationOverride("mysql/5_6/my.conf")
      .withDatabaseName(TestSourceDB.DB_NAME);

  @Override
  protected MySQLContainer<?> getContainer() {
    return mysql;
  }

}
