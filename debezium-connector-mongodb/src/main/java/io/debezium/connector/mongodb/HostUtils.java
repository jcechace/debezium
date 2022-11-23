/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Host string parsing utilities
 */
public final class HostUtils {

    private HostUtils() {
        // intentionally private;
    }

    /**
     * Regular expression that extracts the hosts for the replica sets. The raw expression is
     * {@code ((([^=]+)[=])?(([^/]+)\/))?(.+)}.
     */
    private static final Pattern HOST_PATTERN = Pattern.compile("((([^=]+)[=])?(([^/]+)\\/))?(.+)");

    public static ReplicaSet parse(String hosts) {

        return matcher(hosts).map(m -> {
            String shard = m.group(3);
            String replicaSetName = m.group(5);
            return new ReplicaSet(replicaSetName, shard);
        }).orElse(null);
    }

    public static String parseHost(String hosts) {
        return matcher(hosts).map(m -> m.group(6)).orElse(null);
    }

    private static Optional<Matcher> matcher(String hosts) {
        Objects.requireNonNull(hosts);
        Matcher matcher = HOST_PATTERN.matcher(hosts);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        return Optional.of(matcher);
    }
}
