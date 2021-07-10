package io.debezium.testing.system.tools.kafka.docker;

import org.testcontainers.containers.GenericContainer;

import io.debezium.testing.system.tools.ConfigProperties;

public class ZookeeperContainer extends GenericContainer<ZookeeperContainer> {

    public static final String ZOOKEEPER_COMMAND = "zookeeper";
    public static final int ZOOKEEPER_PORT_CLIENT = 2181;
    public static final int ZOOKEEPER_PORT_PEER = 2888;
    public static final int ZOOKEEPER_PORT_LEADER = 3888;

    public ZookeeperContainer(String containerImageName) {
        super(containerImageName);
        defaultConfig();
    }

    public ZookeeperContainer() {
        this(ConfigProperties.DOCKER_IMAGE_KAFKA_RHEL);
    }

    public String serverAddress() {
        return getNetworkAliases().get(0) + ":" + ZOOKEEPER_PORT_CLIENT;
    }

    private void defaultConfig() {
        withExposedPorts(ZOOKEEPER_PORT_CLIENT);
        withCommand(ZOOKEEPER_COMMAND);
    }

}
