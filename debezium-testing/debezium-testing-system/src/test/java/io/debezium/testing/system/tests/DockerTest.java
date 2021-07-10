package io.debezium.testing.system.tests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Network;

import io.debezium.testing.system.fixtures.DockerNetwork;
import io.debezium.testing.system.fixtures.connectors.MySqlConnector;
import io.debezium.testing.system.fixtures.databases.DockerMySql;
import io.debezium.testing.system.fixtures.kafka.DockerKafka;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DockerTest
        implements DockerKafka, DockerNetwork, DockerMySql, MySqlConnector {

    private KafkaController kafka;
    private KafkaConnectController connect;
    private Network network;
    private ConnectorConfigBuilder connectorConfig;
    private SqlDatabaseController dbController;
    // private MongoDatabaseController dbController;

    @BeforeAll
    public void setup() throws Exception {
        setupNetwork();
        setupKafka();
        setupDatabase();
        setupConnector();
    }

    @AfterAll
    public void teardown() throws Exception {
        teardownConnector();
        teardownKafka();
        teardownDatabase();
        teardownNetwork();
    }

    @Test
    public void test() {
        System.out.println(kafka.getBootstrapAddress());
        System.out.println(kafka.getPublicBootstrapAddress());
        System.out.println(dbController.getDatabaseHostname());
        System.out.println(dbController.getPublicDatabaseHostname());
        System.out.println(dbController.getDatabasePort());
        System.out.println(dbController.getPublicDatabasePort());
        System.out.println(dbController.getPublicDatabaseUrl());

        getConnectorMetrics().waitForMySqlSnapshot(connectorConfig.getDbServerName());
    }

    @Override
    public KafkaConnectController getKafkaConnectController() {
        return connect;
    }

    @Override
    public void setKafkaConnectController(KafkaConnectController controller) {
        connect = controller;
    }

    @Override
    public KafkaController getKafkaController() {
        return kafka;
    }

    @Override
    public void setKafkaController(KafkaController controller) {
        kafka = controller;
    }

    @Override
    public Network getNetwork() {
        return network;
    }

    @Override
    public void setNetwork(Network network) {
        this.network = network;
    }

    @Override
    public SqlDatabaseController getDbController() {
        return dbController;
    }

    @Override
    public void setDbController(SqlDatabaseController controller) {
        this.dbController = controller;
    }

    @Override
    public ConnectorConfigBuilder getConnectorConfig() {
        return connectorConfig;
    }

    @Override
    public void setConnectorConfig(ConnectorConfigBuilder config) {
        this.connectorConfig = config;
    }

    // @Override
    // public MongoDatabaseController getDbController() {
    // return dbController;
    // }
    //
    // @Override
    // public void setDbController(MongoDatabaseController controller) {
    // this.dbController = controller;
    // }
}
