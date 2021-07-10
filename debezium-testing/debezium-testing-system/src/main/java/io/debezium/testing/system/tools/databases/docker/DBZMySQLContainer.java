package io.debezium.testing.system.tools.databases.docker;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

public class DBZMySQLContainer<SELF extends DBZMySQLContainer<SELF>>
        extends MySQLContainer<SELF> {

    private boolean existingDatabase = false;

    public DBZMySQLContainer(String dockerImageName) {
        super(DockerImageName.parse(dockerImageName).asCompatibleSubstituteFor("mysql"));
    }

    public DBZMySQLContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    public SELF withExistingDatabase(String dbName) {
        this.existingDatabase = true;
        return withDatabaseName(dbName);
    }

    @Override
    protected void configure() {
        super.configure();
        if (existingDatabase) {
            getEnvMap().remove("MYSQL_DATABASE");
        }
    }
}
