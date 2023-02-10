/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;
import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.TestUtils.TEST_TABLE_NAME;
import static io.debezium.connector.cassandra.TestUtils.keyspaceTable;
import static io.debezium.connector.cassandra.TestUtils.runCql;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.Maps;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class RowInsertionCommitLogProcessingTest extends AbstractCommitLogProcessorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowInsertionCommitLogProcessingTest.class);

    @Override
    public void initialiseData() throws Exception {

        CassandraConnectorContext context = generateTaskContext();

        createTable("CREATE TABLE IF NOT EXISTS %s.%s (a int, b int, PRIMARY KEY(a)) WITH cdc = true;");
        context.getCassandraClient().execute("CREATE TABLE IF NOT EXISTS " + keyspaceTable("cdc_table")
                + "(key text, key2 int, des text, distance double, conf map<text, text>, primary key(key, key2)) with cdc = true;");
        for (int i = 0; i < 3; i++) {
            runCql(insertInto(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)
                    .value("a", literal(i))
                    .value("b", literal(i))
                    .build());
            Map<String, String> conf = new HashMap<>();
            conf.put("name" + i, "verma");
            runCql(insertInto(TEST_KEYSPACE_NAME, "cdc_table")
                    .value("key", literal("key1"))
                    .value("key2", literal(i))
                    .value("conf", literal(conf))
                    .build());

            runCql(update(TEST_KEYSPACE_NAME, "cdc_table").appendMapEntry("conf", literal("state"), literal("nj"))
                    .whereColumn("key").isEqualTo(literal("key1"))
                    .whereColumn("key2").isEqualTo(literal(i))
                    .build());

            ResultSet res = context.getCassandraClient().execute(selectFrom(TEST_KEYSPACE_NAME, "cdc_table").columns("key", "key2", "conf").whereColumn("key")
                    .isEqualTo(literal("key1")).whereColumn("key2").isEqualTo(literal(i)).build());
            for (Row r : res) {
                LOGGER.error("row: " + r.getFormattedContents());
            }
            Map<String, String> conf2 = new HashMap<>();
            conf2.put("overwrite" + i, "replaceMap");

            runCql(update(TEST_KEYSPACE_NAME, "cdc_table").setColumn("conf", literal(conf2))
                    .whereColumn("key").isEqualTo(literal("key1"))
                    .whereColumn("key2").isEqualTo(literal(i))
                    .build());
            res = context.getCassandraClient().execute(selectFrom(TEST_KEYSPACE_NAME, "cdc_table").columns("key", "key2", "conf").whereColumn("key")
                    .isEqualTo(literal("key1")).whereColumn("key2").isEqualTo(literal(i)).build());

            for (Row r : res) {
                LOGGER.error("AFter overwrite");
                LOGGER.error("row: " + r.getFormattedContents());
            }
        }
    }

    @Override
    public void verifyEvents() throws Exception {
        for (Event event : getEvents(12)) {
            if (event instanceof Record) {
                Record record = (Record) event;

                JsonConverter converter = new JsonConverter();
                converter.configure(Maps.newHashMap(), false);
                LOGGER.error(new String(converter.fromConnectData("test", record.getValueSchema(), record.buildValue())));

                assertEquals(record.getEventType(), Event.EventType.CHANGE_EVENT);
                assertEquals(record.getSource().cluster, DatabaseDescriptor.getClusterName());
                assertFalse(record.getSource().snapshot);
                // assertEquals(record.getSource().keyspaceTable.name(), keyspaceTable(TEST_TABLE_NAME));
            }
            else if (event instanceof EOFEvent) {
                EOFEvent eofEvent = (EOFEvent) event;
                assertFalse(context.getErroneousCommitLogs().contains(eofEvent.file.getName()));
            }
            else {
                throw new Exception("unexpected event type");
            }
        }
    }
}
