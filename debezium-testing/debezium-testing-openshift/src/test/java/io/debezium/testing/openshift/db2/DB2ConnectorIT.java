/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.db2;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import io.debezium.testing.openshift.OcpConnectorTest;
import io.debezium.testing.openshift.fixtures.connectors.Db2Connector;
import io.debezium.testing.openshift.fixtures.databases.OcpDb2;
import io.debezium.testing.openshift.fixtures.kafka.OcpKafka;
import io.debezium.testing.openshift.tools.databases.SqlDatabaseController;

/**
 * @author Jakub Cechacek
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("acceptance")
@Tag("db2 ")
@Tag("openshift")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DB2ConnectorIT
        extends OcpConnectorTest<SqlDatabaseController>
        implements OcpKafka, OcpDb2, Db2Connector, DB2lTestCases {
}
