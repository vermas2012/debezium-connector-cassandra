/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static io.debezium.connector.cassandra.Event.EventType.CHANGE_EVENT;
import static io.debezium.connector.cassandra.Record.Operation.DELETE;
import static io.debezium.connector.cassandra.Record.Operation.INSERT;
import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME_2;
import static io.debezium.connector.cassandra.TestUtils.TEST_TABLE_NAME;
import static io.debezium.connector.cassandra.TestUtils.TEST_TABLE_NAME_2;
import static io.debezium.connector.cassandra.TestUtils.createTestKeyspace;
import static io.debezium.connector.cassandra.TestUtils.runCql;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.Maps;

public class MultipleTablesProcessingTest extends AbstractCommitLogProcessorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultipleTablesProcessingTest.class);

    @Override
    public void initialiseData() throws Exception {
        createTable(
                "CREATE TABLE IF NOT EXISTS %s.%s (a int, b int, c int, PRIMARY KEY ((a), b)) WITH cdc = true;",
                TEST_KEYSPACE_NAME, TEST_TABLE_NAME);
        createTable(
                "CREATE TABLE IF NOT EXISTS %s.%s (a int, b int, c int, d double, PRIMARY KEY ((a), b)) WITH cdc = true;",
                TEST_KEYSPACE_NAME, TEST_TABLE_NAME_2);

        createTestKeyspace(TEST_KEYSPACE_NAME_2);

        createTable(
                "CREATE TABLE IF NOT EXISTS %s.%s (a int, b int, c int, PRIMARY KEY ((a), b)) WITH cdc = true;",
                TEST_KEYSPACE_NAME_2, TEST_TABLE_NAME);
        createTable(
                "CREATE TABLE IF NOT EXISTS %s.%s (a int, b int, c int, PRIMARY KEY ((a), b)) WITH cdc = true;",
                TEST_KEYSPACE_NAME_2, TEST_TABLE_NAME_2);

        runCql(insertInto(TEST_KEYSPACE_NAME, TEST_TABLE_NAME)
                .value("a", literal(1))
                .value("b", literal(1))
                .value("c", literal(1))
                .build());

        runCql(insertInto(TEST_KEYSPACE_NAME, TEST_TABLE_NAME_2)
                .value("a", literal(1))
                .value("b", literal(2))
                .value("c", literal(3))
                .build());

        runCql(insertInto(TEST_KEYSPACE_NAME_2, TEST_TABLE_NAME)
                .value("a", literal(1))
                .value("b", literal(1))
                .value("c", literal(1))
                .build());

        runCql(insertInto(TEST_KEYSPACE_NAME_2, TEST_TABLE_NAME_2)
                .value("a", literal(1))
                .value("b", literal(2))
                .value("c", literal(3))
                .build());

        runCql(deleteFrom(TEST_KEYSPACE_NAME, TEST_TABLE_NAME).whereColumn("a").isEqualTo(literal(1))
                .build());
        runCql(deleteFrom(TEST_KEYSPACE_NAME, TEST_TABLE_NAME_2).whereColumn("a").isEqualTo(literal(1))
                .build());

        runCql(deleteFrom(TEST_KEYSPACE_NAME_2, TEST_TABLE_NAME).whereColumn("a").isEqualTo(literal(1))
                .build());
        runCql(
                deleteFrom(TEST_KEYSPACE_NAME_2, TEST_TABLE_NAME_2).whereColumn("a").isEqualTo(literal(1))
                        .build());

        runCql(insertInto(TEST_KEYSPACE_NAME, TEST_TABLE_NAME_2)
                .value("a", literal(1))
                .value("b", literal(2))
                .value("d", literal(3.0))
                .build());
        runCql(
                deleteFrom(TEST_KEYSPACE_NAME_2, TEST_TABLE_NAME_2).whereColumn("a").isEqualTo(literal(1)).whereColumn("b").isEqualTo(literal(2))
                        .build());

    }

    @Override
    public void verifyEvents() throws Exception {
        final List<Event> events = getEvents(10);

        JsonConverter converter = new JsonConverter();
        converter.configure(Maps.newHashMap(), false);

        Record insert1 = (Record) events.get(0);
        assertEquals(insert1.getEventType(), CHANGE_EVENT);
        assertEquals(INSERT, insert1.getOp());

        Record insert2 = (Record) events.get(1);
        assertEquals(insert2.getEventType(), CHANGE_EVENT);
        assertEquals(INSERT, insert2.getOp());

        Record insert3 = (Record) events.get(2);
        assertEquals(insert3.getEventType(), CHANGE_EVENT);
        assertEquals(INSERT, insert3.getOp());

        Record insert4 = (Record) events.get(3);
        assertEquals(insert4.getEventType(), CHANGE_EVENT);
        assertEquals(INSERT, insert4.getOp());

        Record delete = (Record) events.get(4);
        assertEquals(delete.getEventType(), CHANGE_EVENT);
        assertEquals(DELETE, delete.getOp());

        Record delete2 = (Record) events.get(5);
        assertEquals(delete2.getEventType(), CHANGE_EVENT);
        assertEquals(DELETE, delete2.getOp());

        Record delete3 = (Record) events.get(6);
        assertEquals(delete3.getEventType(), CHANGE_EVENT);
        assertEquals(DELETE, delete3.getOp());

        Record delete4 = (Record) events.get(7);
        assertEquals(delete4.getEventType(), CHANGE_EVENT);
        assertEquals(DELETE, delete4.getOp());
        LOGGER.error(new String(converter.fromConnectData("test", delete4.getValueSchema(), delete4.buildValue())));

        Record record = (Record) events.get(9);
        LOGGER.error(new String(converter.fromConnectData("test", record.getValueSchema(), record.buildValue())));
    }
}
