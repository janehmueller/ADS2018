package de.hpi.ads

import akka.actor.{Address, Props}
import akka.cluster.Cluster
import akka.cluster.MemberStatus.Up
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender

import de.hpi.ads.specs.{MultiNodeADSConfig, STMultiNodeSpec}

import sample.multinode.MultiNodeSample.Ponger

import scala.concurrent.duration.DurationInt

class MultiNodeClusterJoiningTestMultiJvmNode1 extends MultiNodeClusterJoiningTest
class MultiNodeClusterJoiningTestMultiJvmNode2 extends MultiNodeClusterJoiningTest
//class MultiNodeClusterJoiningTestMultiJvmNode3 extends MultiNodeClusterJoiningTest
//class MultiNodeClusterJoiningTestMultiJvmNode4 extends MultiNodeClusterJoiningTest

class MultiNodeClusterJoiningTest extends MultiNodeSpec(MultiNodeADSConfig) with STMultiNodeSpec with ImplicitSender {

    import de.hpi.ads.specs.MultiNodeADSConfig.{node1, node2}

    def initialParticipants: Int = roles.size

    val firstAddress: Address = node(node1).address
    val secondAddress: Address = node(node2).address

    "Cluster Joining Test" should "wait for all nodes to enter a barrier" in {
        enterBarrier("startup")
    }

    it should "illustrate how to start up first node" in {
        runOn(node1) {
            // this will only run on the 'first' node

            Cluster(system) join firstAddress
            // verify that single node becomes member
            awaitCond(Cluster(system).state.members.
                exists(m ⇒
                    m.address == firstAddress && m.status == Up))
        }

        // this will run on all nodes
        // use barrier to coordinate test steps
        testConductor.enter("first-started")
    }

    it should "illustrate join more nodes" in within(10 seconds) {
        runOn(node2) {
            Cluster(system) join firstAddress
        }

        val expected =
            Set(firstAddress, secondAddress)
        // on all nodes, verify that all becomes members
        awaitCond(
            Cluster(system).state.members.
                map(_.address) == expected)
        // and shifted to status Up
        awaitCond(
            Cluster(system).state.members.
                forall(_.status == Up))

        testConductor.enter("all-joined")
    }
}
