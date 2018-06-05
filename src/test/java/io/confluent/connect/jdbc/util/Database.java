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

package io.confluent.connect.jdbc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Database implements HealthCheck<Container> {

  @FunctionalInterface
  public interface DatabaseOperation<T> {
    void apply(T db) throws SQLException;
  }

  private final Logger log = LoggerFactory.getLogger(getClass());

  private Properties connectionProps = new Properties();
  private int maxRetries = 60;
  private long retryDelayMs = 1000;
  private long timeoutMs = TimeUnit.SECONDS.toMillis(60);
  private int port = 3306;
  private String host = "localhost";
  private String healthCheckQuery = "show databases";

  public Database user(String user) {
    connectionProps.setProperty("user", user);
    return this;
  }

  public Database password(String password) {
    connectionProps.setProperty("password", password);
    return this;
  }

  public Database host(String host) {
    this.host = host;
    return this;
  }

  public Database port(int port) {
    this.port = port;
    return this;
  }

  public Database query(String healthCheckQuery) {
    Objects.nonNull(healthCheckQuery);
    this.healthCheckQuery = healthCheckQuery;
    return this;
  }

  public Database retryDelay(long delay, TimeUnit unit) {
    this.retryDelayMs = unit.toMillis(delay);
    return this;
  }

  public Database timeout(long timeout, TimeUnit unit) {
    this.timeoutMs = unit.toMillis(timeout);
    return this;
  }

  public Database maxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  public String url() {
    return String.format("jdbc:mysql://%s:%d", host, port);
  }

  public void onConnection(DatabaseOperation<Connection> action) throws SQLException {
    String url = url();
    log.trace("Attempting to connect to {}", url);
    try (Connection conn = DriverManager.getConnection(url, connectionProps)) {
      action.apply(conn);
      log.trace("Closing connection to {}", url);
    }
  }

  public void execute(DatabaseOperation<Statement> action) throws SQLException {
    onConnection(conn -> {
      log.trace("Creating statement using connection to {}", url());
      try (Statement statement = conn.createStatement()) {
        action.apply(statement);
        log.trace("Closing statement from connection to {}", url());
      }
    });
  }

  public void execute(String sql, DatabaseOperation<ResultSet> consumeResults) throws SQLException {
    execute(statement -> {
      log.trace("Executing query '{}' using connection to {}", sql, url());
      try (ResultSet rs = statement.executeQuery(sql)) {
        consumeResults.apply(rs);
        log.trace("Closing result set for '{}' from connection to {}", sql, url());
      }
    });
  }

  public SuccessOrFailure isHealthy(Container container) {
    DockerPort hostAndPort = container.portMappedExternallyTo(port);
    host(hostAndPort.getIp());
    port(hostAndPort.getExternalPort());
    String url = url();
    log.info("Trying to connecto to {}", url);
    final long startTime = System.currentTimeMillis();

    AtomicBoolean success = new AtomicBoolean(false);
    AtomicInteger attempt = new AtomicInteger();
    while (!success.get() && attempt.getAndIncrement() < maxRetries && now() - startTime > timeoutMs) {
      try {
        log.debug("Attempt {} of {} to connect to {}", attempt, maxRetries, url);
        execute(healthCheckQuery, (rs) -> {
          if (rs.next()) {
            success.set(true);
            return;
          }
        });
      } catch (SQLException e) {
        log.trace(
            "Error running health check query '{}': code={}, reason={}",
            healthCheckQuery,
            e.getErrorCode(),
            e.getMessage()
        );
      }
    }

    if (success.get()) {
      log.info(
          "Successfully connected to {} after {} attempts and {} seconds",
          url,
          attempt,
          toSeconds(now() - startTime)
      );
      return SuccessOrFailure.success();
    }

    return SuccessOrFailure.failure(
        String.format(
            "Unable to connect to %s after %d retries and %d seconds",
            url,
            maxRetries,
            toSeconds(now() - startTime)
        )
    );
  }

  private long now() {
    return System.currentTimeMillis();
  }

  private double toSeconds(long milliseconds) {
    return milliseconds / 1000.0d;
  }
}
