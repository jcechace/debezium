/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;

import io.debezium.config.Configuration;

/**
 * @author Randall Hauch
 *
 */
public class ConnectionContext implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionContext.class);

    protected final Configuration config;
    protected final MongoClients pool;
    protected final boolean useHostsAsSeeds;

    /**
     * @param config the configuration
     */
    public ConnectionContext(Configuration config) {
        this.config = config;

        this.useHostsAsSeeds = config.getBoolean(MongoDbConnectorConfig.AUTO_DISCOVER_MEMBERS);
        final String username = config.getString(MongoDbConnectorConfig.USER);
        final String password = config.getString(MongoDbConnectorConfig.PASSWORD);
        final String adminDbName = config.getString(MongoDbConnectorConfig.AUTH_SOURCE);
        final boolean useSSL = config.getBoolean(MongoDbConnectorConfig.SSL_ENABLED);
        final boolean sslAllowInvalidHostnames = config.getBoolean(MongoDbConnectorConfig.SSL_ALLOW_INVALID_HOSTNAMES);

        final int connectTimeoutMs = config.getInteger(MongoDbConnectorConfig.CONNECT_TIMEOUT_MS);
        final int heartbeatFrequencyMs = config.getInteger(MongoDbConnectorConfig.HEARTBEAT_FREQUENCY_MS);
        final int socketTimeoutMs = config.getInteger(MongoDbConnectorConfig.SOCKET_TIMEOUT_MS);
        final int serverSelectionTimeoutMs = config.getInteger(MongoDbConnectorConfig.SERVER_SELECTION_TIMEOUT_MS);

        // Set up the client pool so that it ...
        MongoClients.Builder clientBuilder = MongoClients.create();

        clientBuilder.settings()
                .applyToSocketSettings(builder -> builder.connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                        .readTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToClusterSettings(
                        builder -> builder.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToServerSettings(
                        builder -> builder.heartbeatFrequency(heartbeatFrequencyMs, TimeUnit.MILLISECONDS));

        // Use credentials if provided as part of connection String
        final ConnectionString connectionString = connectionString();
        if (connectionString.getCredential() != null) {
            clientBuilder.withCredential(connectionString.getCredential());
        }

        // Use credential if provided as properties
        if (username != null || password != null) {
            clientBuilder.withCredential(MongoCredential.createCredential(username, adminDbName, password.toCharArray()));
        }
        if (useSSL) {
            clientBuilder.settings().applyToSslSettings(
                    builder -> builder.enabled(true).invalidHostNameAllowed(sslAllowInvalidHostnames));
        }

        clientBuilder.settings()
                .applyToSocketSettings(builder -> builder.connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                        .readTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToClusterSettings(
                        builder -> builder.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS));

        pool = clientBuilder.build();
    }

    public void shutdown() {
        try {
            // Closing all connections ...
            logger().info("Closing all connections to {}", connectionSeed());
            pool.clear();
        }
        catch (Throwable e) {
            logger().error("Unexpected error shutting down the MongoDB clients", e);
        }
    }

    @Override
    public final void close() {
        shutdown();
    }

    protected Logger logger() {
        return LOGGER;
    }

    public MongoClients clients() {
        return pool;
    }

    public boolean performSnapshotEvenIfNotNeeded() {
        return false;
    }

    public MongoClient clientForSeedConnection() {
        return pool.clientFor(connectionString());
    }

    private MongoClient clientFor(ReplicaSet replicaSet, ReadPreference preference) {
        return pool.clientFor(connectionString(), replicaSet, preference);
    }

    public String hosts() {
        return config.getString(MongoDbConnectorConfig.HOSTS);
    }

    /**
     * Initial connection seed which is either a host specification or connection string
     * @return hosts or connection string
     */
    public String connectionSeed() {
        String seed = config.getString(MongoDbConnectorConfig.CONNECTION_STRING);
        if (seed == null) {
            String hosts = config.getString(MongoDbConnectorConfig.HOSTS);
            var host = HostUtils.parseHost(hosts);
            seed = String.format("mongodb://%s", host);
        }
        return seed;
    }

    private ConnectionString connectionString() {
        return new ConnectionString(connectionSeed());
    }

    public Duration pollInterval() {
        return Duration.ofMillis(config.getLong(MongoDbConnectorConfig.MONGODB_POLL_INTERVAL_MS));
    }

    /**
     * Obtain a client that will repeatedly try to obtain a client to the primary node of the replica set, waiting (and using
     * this context's back-off strategy) if required until the primary becomes available.
     *
     * @param replicaSet the replica set information; may not be null
     * @param filters the filter configuration
     * @param errorHandler the function to be called whenever the primary is unable to
     *            {@link RetryingMongoClient#execute(String, Consumer) execute} an operation to completion; may be null
     * @return the client, or {@code null} if no primary could be found for the replica set
     */
    public RetryingMongoClient primaryFor(ReplicaSet replicaSet, Filters filters, BiConsumer<String, Throwable> errorHandler) {
        return preferredFor(replicaSet, ReadPreference.primary(), filters, errorHandler);
    }

    /**
     * Obtain a client that will repeatedly try to obtain a client to a node of preferred type of the replica set, waiting (and using
     * this context's back-off strategy) if required until the node becomes available.
     *
     * @param replicaSet the replica set information; may not be null
     * @param filters the filter configuration
     * @param errorHandler the function to be called whenever the node is unable to
     *            {@link RetryingMongoClient#execute(String, Consumer) execute} an operation to completion; may be null
     * @return the client, or {@code null} if no primary could be found for the replica set
     */
    public RetryingMongoClient preferredFor(ReplicaSet replicaSet, ReadPreference preference, Filters filters,
                                            BiConsumer<String, Throwable> errorHandler) {
        return new RetryingMongoClient(replicaSet, preference, this::clientFor, filters, errorHandler);
    }

}
