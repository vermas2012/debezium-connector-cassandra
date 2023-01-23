/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.kafka.ProducerSettings;

import io.debezium.connector.cassandra.exceptions.CassandraConnectorConfigException;

public class ComponentFactoryStandalone implements ComponentFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraConnectorTaskTemplate.class);

    @Override
    public OffsetWriter offsetWriter(CassandraConnectorConfig config) {
        try {
            return new FileOffsetWriter(config.offsetBackingStoreDir());
        }
        catch (IOException e) {
            throw new CassandraConnectorConfigException(String.format("cannot create file offset writer into %s", config.offsetBackingStoreDir()), e);
        }
    }

    @Override
    public Emitter recordEmitter(CassandraConnectorContext context) {
        try {

            LOGGER.error("#### Creating pubsub emitter in the factory");

            CassandraConnectorConfig config = context.getCassandraConnectorConfig();
            // TODO(vermas2012): Create a PubSub connector based on the config
            TopicPath topicPath = TopicPath.newBuilder()
                    .setLocation(CloudZone.of(CloudRegion.of("us-east1"), 'b'))
                    .setProject(ProjectId.of("google.com:cloud-bigtable-dev"))
                    .setName(TopicName.of("test-shitanshu"))
                    .build();

            LOGGER.error("##### Created topic path");

            ProducerSettings producerSettings = ProducerSettings.newBuilder().setTopicPath(topicPath).build();

            LOGGER.error("####### Finished creating production settings, about to create emitter now.");
            return new KafkaRecordEmitter(
                    config,
                    producerSettings.instantiate(),
                    context.getOffsetWriter(),
                    config.offsetFlushIntervalMs(),
                    config.maxOffsetFlushSize(),
                    config.getKeyConverter(),
                    config.getValueConverter(),
                    context.getErroneousCommitLogs(),
                    config.getCommitLogTransfer());
        }
        catch (Exception e) {

            LOGGER.error("### Failed to create KafkaEmitter, ", e);
        }

        throw new RuntimeException("#### There were error in creating kafka emitter.");
    }

}
