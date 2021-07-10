package io.debezium.testing.system.tools.databases.mysql;

import org.testcontainers.containers.MySQLContainer;

import io.debezium.testing.system.tools.databases.AbstractDockerSqlDatabaseController;
import io.debezium.testing.system.tools.databases.docker.DBZMySQLContainer;

public class DockerMysqlController extends AbstractDockerSqlDatabaseController<DBZMySQLContainer<?>> {

    DockerMysqlController(DBZMySQLContainer<?> container) {
        super(container);
    }

    @Override
    public int getDatabasePort() {
        return MySQLContainer.MYSQL_PORT;
    }
}
