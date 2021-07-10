package io.debezium.testing.system.fixtures;

import org.testcontainers.containers.Network;

public interface DockerNetwork {

    default void setupNetwork() {
        setNetwork(Network.newNetwork());
    }

    default void teardownNetwork() {
        getNetwork().close();
    }

    void setNetwork(Network network);

    Network getNetwork();
}
