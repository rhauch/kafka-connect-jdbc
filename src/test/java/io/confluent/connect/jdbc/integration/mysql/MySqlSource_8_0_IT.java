/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.jdbc.integration.mysql;

import org.junit.ClassRule;
import org.testcontainers.containers.MySQLContainer;

public class MySqlSource_8_0_IT extends MySqlSourceBaseIT {

  @ClassRule
  public static final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0")
      .withConfigurationOverride("mysql/8_0/my.conf")
      .withDatabaseName(TestSourceDB.DB_NAME);

  @Override
  protected MySQLContainer<?> getContainer() {
    return mysql;
  }

}
