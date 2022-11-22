/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
public class ReplicaSetsTest {

    @Test
    public void shouldNotSubdivideOneReplicaSet() {
        // sets = ReplicaSets.parse("rs1/host1:27017,host2:27017");
        // List<ReplicaSets> divided = new ArrayList<>();
        // sets.subdivide(1, divided::add);
        // assertThat(divided.size()).isEqualTo(1);
        // assertThat(divided.get(0)).isSameAs(sets);
    }

    @Test
    public void shouldNotSubdivideMultipleReplicaSetsIntoOneGroup() {
        // sets = ReplicaSets.parse("rs1/host1:27017,host2:27017;rs2/host3:27017");
        // List<ReplicaSets> divided = new ArrayList<>();
        // sets.subdivide(1, divided::add);
        // assertThat(divided.size()).isEqualTo(1);
        // assertThat(divided.get(0)).isSameAs(sets);
    }

    @Test
    public void shouldSubdivideMultipleReplicaSetsWithIntoMultipleGroups() {
        // sets = ReplicaSets.parse("rs1/host1:27017,host2:27017;rs2/host3:27017");
        // List<ReplicaSets> divided = new ArrayList<>();
        // sets.subdivide(2, divided::add);
        // assertThat(divided.size()).isEqualTo(2);
        //
        // ReplicaSets dividedSet1 = divided.get(0);
        // assertThat(dividedSet1.replicaSetCount()).isEqualTo(1);
        // assertThat(dividedSet1.all().get(0)).isSameAs(sets.all().get(0));
        //
        // ReplicaSets dividedSet2 = divided.get(1);
        // assertThat(dividedSet2.replicaSetCount()).isEqualTo(1);
        // assertThat(dividedSet2.all().get(0)).isSameAs(sets.all().get(1));
    }

}
