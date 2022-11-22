/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.kafka.connect.util.ConnectorUtils;

import io.debezium.annotation.Immutable;
import io.debezium.util.Strings;

/**
 * A set of replica set specifications.
 *
 * @author Randall Hauch
 */
@Immutable
public class ReplicaSets {

    private static final Pattern REPLICA_DELIMITER_PATTERN = Pattern.compile(";");

    /**
     * Get an instance that contains no replica sets.
     *
     * @return the empty instance; never null
     */
    public static ReplicaSets empty() {
        return new ReplicaSets(null);
    }

    private final Map<String, ReplicaSet> replicaSetsByName = new HashMap<>();
    private final List<ReplicaSet> replicaSets = new ArrayList<>();

    /**
     * Create a set of replica set specifications.
     *
     * @param rsSpecs the replica set specifications; may be null or empty
     */
    public ReplicaSets(Collection<ReplicaSet> rsSpecs) {
        if (rsSpecs != null) {
            replicaSets.addAll(rsSpecs);
        }
        Collections.sort(replicaSets);
    }

    /**
     * Get the number of replica sets.
     *
     * @return the replica set count
     */
    public int size() {
        return replicaSets.size();
    }

    /**
     * Perform the supplied function on each of the replica sets
     *
     * @param function the consumer function; may not be null
     */
    public void onEachReplicaSet(Consumer<ReplicaSet> function) {
        this.replicaSets.forEach(function);
    }

    /**
     * Subdivide this collection of replica sets into the maximum number of groups.
     *
     * @param maxSubdivisionCount the maximum number of subdivisions
     * @param subdivisionConsumer the function to be called with each subdivision; may not be null
     */
    public void subdivide(int maxSubdivisionCount, Consumer<ReplicaSets> subdivisionConsumer) {
        int numGroups = Math.min(size(), maxSubdivisionCount);
        if (numGroups <= 1) {
            // Just one replica set or subdivision ...
            subdivisionConsumer.accept(this);
            return;
        }
        ConnectorUtils.groupPartitions(all(), numGroups).forEach(rsList -> {
            subdivisionConsumer.accept(new ReplicaSets(rsList));
        });
    }

    /**
     * Get a copy of all of the {@link ReplicaSet} objects.
     *
     * @return the replica set objects; never null but possibly empty
     */
    public List<ReplicaSet> all() {
        List<ReplicaSet> replicaSets = new ArrayList<>();
        replicaSets.addAll(replicaSetsByName.values());
        replicaSets.addAll(this.replicaSets);
        return replicaSets;
    }

    /**
     * Get a copy of all of the {@link ReplicaSet} objects that have no names.
     *
     * @return the unnamed replica set objects; never null but possibly empty
     */
    public List<ReplicaSet> unnamedReplicaSets() {
        List<ReplicaSet> replicaSets = new ArrayList<>();
        replicaSets.addAll(this.replicaSets);
        return replicaSets;
    }

    /**
     * Determine if one or more replica sets has been added or removed since the prior state.
     *
     * @param priorState the prior state of the replica sets; may be null
     * @return {@code true} if the replica sets have changed since the prior state, or {@code false} otherwise
     */
    public boolean haveChangedSince(ReplicaSets priorState) {
        return !this.replicaSets.equals(priorState.replicaSets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(replicaSetsByName, replicaSets);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ReplicaSets) {
            ReplicaSets that = (ReplicaSets) obj;
            return this.replicaSetsByName.equals(that.replicaSetsByName) && this.replicaSets.equals(that.replicaSets);
        }
        return false;
    }

    @Override
    public String toString() {
        return Strings.join(";", all());
    }

}
