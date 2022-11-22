/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.debezium.annotation.Immutable;

@Immutable
public final class ReplicaSet implements Comparable<ReplicaSet> {

    /**
     * Regular expression that extracts the hosts for the replica sets. The raw expression is
     * {@code ((([^=]+)[=])?(([^/]+)\/))?(.+)}.
     */
    private static final Pattern HOST_PATTERN = Pattern.compile("((([^=]+)[=])?(([^/]+)\\/))?(.+)");

    /**
     * Parse the supplied string for the information about the hosts for a replica set. The string is a shard host
     * specification (e.g., "{@code shard01=replicaSet1/host1:27017,host2:27017}"), replica set hosts (e.g.,
     * "{@code replicaSet1/host1:27017,host2:27017}"), or standalone host (e.g., "{@code host1:27017}" or
     * "{@code 1.2.3.4:27017}").
     *
     * @param hosts the hosts string; may be null
     * @return the replica set; or {@code null} if the host string could not be parsed
     */
    public static ReplicaSet parse(String hosts) {
        if (hosts != null) {
            Matcher matcher = HOST_PATTERN.matcher(hosts);
            if (matcher.matches()) {
                String shard = matcher.group(3);
                String replicaSetName = matcher.group(5);
                return new ReplicaSet(replicaSetName, shard);
            }
        }
        return null;
    }

    public static String parseHost(String hosts) {
        Matcher matcher = HOST_PATTERN.matcher(hosts);
        if (!matcher.matches()) {
            return null;
        }
        return matcher.group(6);
    }

    private final String replicaSetName;
    private final String shardName;
    private final int hc;

    public ReplicaSet(String replicaSetName) {
        this(replicaSetName, replicaSetName);
    }

    public ReplicaSet(String replicaSetName, String shardName) {
        this.replicaSetName = replicaSetName;
        this.shardName = shardName;
        this.hc = Objects.hash(replicaSetName, shardName);
    }

    /**
     * Get the name of this replica set.
     *
     * @return the replica set name, or {@code null} if the addresses are for standalone servers.
     */
    public String replicaSetName() {
        return replicaSetName;
    }

    /**
     * Get the shard name for this replica set.
     *
     * @return the shard name, or {@code null} if this replica set is not used as a shard
     */
    public String shardName() {
        return shardName;
    }

    /**
     * Return whether the address(es) represents a replica set, where the {@link #replicaSetName() replica set name} is
     * not {@code null}.
     *
     * @return {@code true} if this represents the address of a replica set, or {@code false} if it represents the
     *         address of a standalone server
     */
    public boolean hasReplicaSetName() {
        return replicaSetName != null;
    }

    @Override
    public int hashCode() {
        return hc;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ReplicaSet) {
            ReplicaSet that = (ReplicaSet) obj;
            return Objects.equals(this.shardName, that.shardName) && Objects.equals(this.replicaSetName, that.replicaSetName);
        }
        return false;
    }

    @Override
    public int compareTo(ReplicaSet that) {
        if (that == this) {
            return 0;
        }

        return replicaSetName.compareTo(that.replicaSetName);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (this.shardName != null && !this.shardName.isEmpty()) {
            sb.append(shardName).append('=');
        }
        if (this.replicaSetName != null && !this.replicaSetName.isEmpty()) {
            sb.append(replicaSetName).append('/');
        }
        return sb.toString();
    }

}
