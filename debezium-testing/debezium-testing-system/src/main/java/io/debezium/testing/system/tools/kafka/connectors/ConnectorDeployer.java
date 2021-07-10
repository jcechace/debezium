package io.debezium.testing.system.tools.kafka.connectors;

import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;

public interface ConnectorDeployer {

    /**
     * Deploys Kafka connector with given configuration
     *
     * @param config connector config
     */
    void deploy(ConnectorConfigBuilder config);

    void undeploy(String name);

    default void undeploy(ConnectorConfigBuilder config) {
        undeploy(config.getConnectorName());
    }
}
