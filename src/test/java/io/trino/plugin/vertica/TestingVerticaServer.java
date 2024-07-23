/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.vertica;

import io.trino.testing.ResourcePresence;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.containers.TestContainers.startOrReuse;
import static java.lang.String.format;
import static java.util.function.Predicate.not;

public class TestingVerticaServer
        implements AutoCloseable
{
    private static final String USER = "dbadmin";
    private static final String PASSWORD = "";
    private static final String DATABASE = "VMart";
    public static final Integer VSQL_PORT = 5433;
    private final GenericContainer<?> dockerContainer;

    private final Closeable cleanup;

    public TestingVerticaServer()
    {
        this(false);
    }

    public TestingVerticaServer(boolean shouldExposeFixedPorts)
    {
        dockerContainer = new GenericContainer<>(DockerImageName.parse("opentext/vertica-ce:latest"))
                .withExposedPorts(VSQL_PORT);
//                .withLogConsumer(new RemoteDatabaseEventLogConsumer())
//                .withCommand("/opt/vertica/sbin/vertica", "-D", "data", "-p", PASSWORD, "-d", DATABASE);
        cleanup = startOrReuse(dockerContainer);
        dockerContainer.start();
        //execute("CREATE SCHEMA TRINO;");
        execute("CREATE SCHEMA IF NOT EXISTS trino;");
        execute("CREATE TABLE trino.test (i int,f float,d date,ts timestamp,v varchar(80)\n);");
        execute("CREATE TABLE trino.precision_1000 (id int, amount  NUMERIC(1000,200));");
        execute("CREATE TABLE trino.precision (id int, amount  NUMERIC(37, 15));");
        execute("CREATE TABLE trino.varbinary (id int, amount  VARBINARY(9999));");

        execute("INSERT INTO trino.precision_1000 (id, amount) VALUES (1, 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999);");
        execute("INSERT INTO trino.precision (id, amount) VALUES (1, 999.999);");
        execute("CREATE VIEW trino.v_precision_1000 AS SELECT * FROM trino.precision_1000;");
        execute("CREATE VIEW trino.v_precision AS SELECT * FROM trino.precision;");
        execute("INSERT INTO trino.varbinary (id, amount) VALUES (1, 'abababababababababababa'::VARBINARY);");

        execute("INSERT INTO trino.test VALUES (1,1.23,current_date,current_timestamp,'Trino');");
        execute("INSERT INTO trino.test VALUES (2,2.34,current_date,current_timestamp,'Vertica');");
        execute("INSERT INTO trino.test SELECT i+1,f+1.1,d,ts,v from trino.test;");
        execute("INSERT INTO trino.test SELECT i+2,f+2.2,d,ts,v from trino.test;");
//        cleanup = dockerContainer::close;

//        execute("CREATE SCHEMA IF NOT EXISTS trino;");
    }

//    private class RemoteDatabaseEventLogConsumer
//            implements Consumer<OutputFrame>
//    {
//        private boolean cancellationHit;
//
//        @Override
//        public void accept(OutputFrame outputFrame)
//        {
//            if (tracingEvents.isEmpty()) {
//                return;
//            }
//
//            buildEvent(outputFrame)
//                    .ifPresent(remoteDatabaseEvent -> tracingEvents.forEach(tracingEvent -> tracingEvent.accept(remoteDatabaseEvent)));
//        }

    public void execute(@Language("SQL") String sql)
    {
        execute(getJdbcUrl(), getProperties(), sql);
    }

    private static void execute(String url, Properties properties, String sql)
    {
        try (Connection connection = DriverManager.getConnection(url, properties);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

//    protected List<RemoteDatabaseEvent> getRemoteDatabaseEvents()
//    {
//        List<String> logs = getLogs();
//        Iterator<String> logsIterator = logs.iterator();
//        ImmutableList.Builder<RemoteDatabaseEvent> events = ImmutableList.builder();
//        while (logsIterator.hasNext()) {
//            String logLine = logsIterator.next().replaceAll(LOG_PREFIX_REGEXP, "");
//            if (logLine.startsWith(LOG_RUNNING_STATEMENT_PREFIX)) {
//                Matcher matcher = SQL_QUERY_FIND_PATTERN.matcher(logLine.substring(LOG_RUNNING_STATEMENT_PREFIX.length()));
//                if (matcher.find()) {
//                    String sqlStatement = matcher.group(2);
//                    events.add(new RemoteDatabaseEvent(sqlStatement, RUNNING));
//                }
//            }
//            if (logLine.equals(LOG_CANCELLATION_EVENT)) {
//                // next line must be present
//                String cancelledStatementLogLine = logsIterator.next().replaceAll(LOG_PREFIX_REGEXP, "");
//                if (cancelledStatementLogLine.startsWith(LOG_CANCELLED_STATEMENT_PREFIX)) {
//                    events.add(new RemoteDatabaseEvent(cancelledStatementLogLine.substring(LOG_CANCELLED_STATEMENT_PREFIX.length()), CANCELLED));
//                }
//            }
//            // ignore unsupported log lines
//        }
//        return events.build();
//    }

    private List<String> getLogs()
    {
        return Stream.of(dockerContainer.getLogs().split("\n"))
                .filter(not(String::isBlank))
                .collect(toImmutableList());
    }

    public String getUser()
    {
        return USER;
    }

    public String getPassword()
    {
        return PASSWORD;
    }

    public Properties getProperties()
    {
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("password", PASSWORD);
        properties.setProperty("searchpath", "public");
        return properties;
    }

    public String getJdbcUrl()
    {
        return format("jdbc:vertica://%s:%s/%s", dockerContainer.getHost(), dockerContainer.getMappedPort(VSQL_PORT), DATABASE);
    }

    @Override
    public void close()
    {
        try {
            cleanup.close();
        }
        catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return dockerContainer.getContainerId() != null;
    }
}
