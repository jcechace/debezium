package io.debezium.testing.system.tests.mongodb;


import fixture5.FixtureExtension;
import fixture5.annotations.Fixture;
import io.debezium.testing.system.TestUtils;
import io.debezium.testing.system.assertions.KafkaAssertions;
import io.debezium.testing.system.fixtures.OcpClient;
import io.debezium.testing.system.fixtures.connectors.ShardedMongoConnector;
import io.debezium.testing.system.fixtures.connectors.ShardedReplicaMongoConnector;
import io.debezium.testing.system.fixtures.databases.ocp.OcpMongoSharded;
import io.debezium.testing.system.fixtures.kafka.OcpKafka;
import io.debezium.testing.system.fixtures.operator.OcpStrimziOperator;
import io.debezium.testing.system.resources.ConnectorFactories;
import io.debezium.testing.system.tests.ConnectorTest;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseClient;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;
import io.debezium.testing.system.tools.databases.mongodb.OcpMongoShardedController;
import io.debezium.testing.system.tools.kafka.ConnectorConfigBuilder;
import io.debezium.testing.system.tools.kafka.KafkaConnectController;
import io.debezium.testing.system.tools.kafka.KafkaController;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import static com.mongodb.client.model.Filters.eq;
import static io.debezium.testing.system.assertions.KafkaAssertions.awaitAssert;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_DBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_LOGIN_DBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MONGO_DBZ_USERNAME;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("acceptance")
@Tag("mongo")
@Tag("openshift")
@Tag("mongo-sharded")
@Fixture(OcpClient.class)
@Fixture(OcpStrimziOperator.class)
@Fixture(OcpKafka.class)
@Fixture(OcpMongoSharded.class)
@Fixture(ShardedReplicaMongoConnector.class)
@ExtendWith(FixtureExtension.class)
public class OcpShardedReplicaMongoConnectorIT extends ConnectorTest {

    public OcpShardedReplicaMongoConnectorIT(KafkaController kafkaController, KafkaConnectController connectController, ConnectorConfigBuilder connectorConfig,
                                      KafkaAssertions<?, ?> assertions) {
        super(kafkaController, connectController, connectorConfig, assertions);
    }

    public void insertCustomer(MongoDatabaseController dbController, String firstName, String lastName, String email, long id) {
        MongoDatabaseClient client = dbController
                .getDatabaseClient(DATABASE_MONGO_DBZ_USERNAME, DATABASE_MONGO_DBZ_PASSWORD, DATABASE_MONGO_DBZ_LOGIN_DBNAME);

        client.execute(DATABASE_MONGO_DBZ_DBNAME, "customers", col -> {
            Document doc = new Document()
                    .append("_id", id)
                    .append("first_name", firstName)
                    .append("last_name", lastName)
                    .append("email", email);
            col.insertOne(doc);
        });
    }

    public void removeCustomer(MongoDatabaseController dbController, String email) {
        MongoDatabaseClient client = dbController
                .getDatabaseClient(DATABASE_MONGO_DBZ_USERNAME, DATABASE_MONGO_DBZ_PASSWORD, DATABASE_MONGO_DBZ_LOGIN_DBNAME);

        client.execute("inventory", "customers", col -> {
            Bson query = eq("email", email);
            col.deleteOne(col.find(query).first());
        });
    }

    public void insertProduct(MongoDatabaseController dbController, String name, String description, String weight, int quantity) {
        MongoDatabaseClient client = dbController
                .getDatabaseClient(DATABASE_MONGO_DBZ_USERNAME, DATABASE_MONGO_DBZ_PASSWORD, DATABASE_MONGO_DBZ_LOGIN_DBNAME);

        client.execute(DATABASE_MONGO_DBZ_DBNAME, "products", col -> {
            Document doc = new Document()
                    .append("name", name)
                    .append("description", description)
                    .append("weight", weight)
                    .append("quantity", quantity);
            col.insertOne(doc);
        });
    }

    private void addAndRemoveShardTest(OcpMongoShardedController dbController, String connectorName) throws IOException, InterruptedException {
        String topic = connectorName + ".inventory.customers";
        int rangeStart = 1100;
        int rangeEnd = 1105;

        // add shard, restart connector, insert to that shard and verify that insert was captured by debezium
        dbController.addShard(3, "THREE", rangeStart, rangeEnd);

        connectController.undeployConnector(connectorName);
        connectController.deployConnector(connectorConfig);

        insertCustomer(dbController, "Filip", "Foobar", "ffoo@test.com", 1101);

        awaitAssert(() -> assertions.assertRecordsContain(topic, "ffoo@test.com"));

        // remove shard, restart connector and verify debezium is still streaming
        removeCustomer(dbController, "ffoo@test.com");
        dbController.removeShard(3, rangeStart, rangeEnd);

        connectController.undeployConnector(connectorName);
        connectController.deployConnector(connectorConfig);
    }



    @Test
    @Order(110)
    public void shouldStreamInReplicaSetMode(OcpMongoShardedController dbController) throws IOException, InterruptedException {
        String topic = connectorConfig.getConnectorName() + ".inventory.customers";
        assertions.assertTopicsExist(
                connectorConfig.getConnectorName() + ".inventory.customers");

        insertCustomer(dbController, "Eve", "Sharded", "eshard@test.com", 1007);

        awaitAssert(() -> assertions.assertRecordsContain(topic, "eshard@test.com"));
        awaitAssert(() -> assertions.assertMinimalRecordsCount(topic, 7));

        insertProduct(dbController, "replicaset product", "demonstrates that replicaset connector mode works", "12.5", 3);
        awaitAssert(() -> assertions.assertRecordsContain(connectorConfig.getConnectorName() + ".inventory.products", "replicaset product"));

        addAndRemoveShardTest(dbController, connectorConfig.getConnectorName());

        insertCustomer(dbController, "Eric", "Eh", "ee@test.com", 1008);
        awaitAssert(() -> assertions.assertRecordsContain(topic, "ee@test.com"));
    }
}
