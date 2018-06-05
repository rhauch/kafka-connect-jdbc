/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.source;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import io.confluent.connect.jdbc.util.Database;
import io.confluent.connect.jdbc.util.MySqlIntegrationTest;

import com.palantir.docker.compose.DockerComposeRule;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

@Category({MySqlIntegrationTest.class})
public class MySqlSometingIntegrationTest {

  private final static Logger LOG = LoggerFactory.getLogger(MySqlIntegrationTest.class);
  private static Database DB = new Database().user("dbuser")
                                                .password("dbpassword");

  @ClassRule
  public static DockerComposeRule docker =
      DockerComposeRule.builder()
                       .file("src/test/resources/mysql/docker-compose.yml")
                       .waitingForService("mysql", DB)
                       .build();

  @Test
  public void shouldGetDatabaseNames() throws SQLException {
    Set<String> dbNames = new HashSet<>();
    DB.execute("show databases", (rs) -> dbNames.add(rs.getString(1)));
    LOG.info("Available databases: {}", dbNames);
    assertTrue(!dbNames.isEmpty());
  }

  @Test
  public void failure() {
//    assertTrue(false);
  }
}
