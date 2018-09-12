package de.hpi.ads

import akka.actor.Address
import akka.cluster.Cluster
import akka.cluster.MemberStatus.Up
import de.hpi.ads.database.types.TableSchema
import de.hpi.ads.remote.actors.TableActor
import de.hpi.ads.remote.messages._
import de.hpi.ads.database.operators._
import de.hpi.ads.specs.{MultiNodeADSBinaryConfig, MultiNodeADSBpConfig, MultiNodeADSConfig, STMultiNodeSpec}

import scala.concurrent.duration.DurationInt
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender

import scala.util.Random

class MultiNodeRandBpTestMultiJvmNode1 extends MultiNodeRandBpTest
class MultiNodeRandBpTestMultiJvmNode2 extends MultiNodeRandBpTest
class MultiNodeRandBpTestMultiJvmNode3 extends MultiNodeRandBpTest
class MultiNodeRandBpTestMultiJvmNode4 extends MultiNodeRandBpTest

class MultiNodeRandBpTest extends MultiNodeSpec(MultiNodeADSBpConfig) with STMultiNodeSpec with ImplicitSender {

    import MultiNodeADSBpConfig._

    var counter: Int = 0
    def tableName: String = {
        counter += 1
        s"tableRandBpTest_$counter"
    }
    def initialParticipants: Int = roles.size

    val firstAddress: Address = node(node1).address
    val secondAddress: Address = node(node2).address
    val thirdAddress: Address = node(node3).address
    val fourthAddress: Address = node(node4).address



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
        runOn(node3) {
            Cluster(system) join firstAddress
        }
        runOn(node4) {
            Cluster(system) join firstAddress
        }

        val expected =
            Set(firstAddress, secondAddress, thirdAddress, fourthAddress)
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

    "MultiNode Table" should "insert and return values" in within(1000 seconds) {
        runOn(node1) {
            println("Test RandBp")
            val RNG = new Random()
            val schema = TableSchema("id:int;title:string(20)")
            val row = List(1, "Great Movie")
            val tableActor = system.actorOf(TableActor.props(tableName, schema))
            Thread.sleep(1000)
            val rang = 1 to 20000 toBuffer
            val randRang = RNG.shuffle(rang)
            val t0 = System.nanoTime()
            val msgCount = 20000
            for (i <- 1 to msgCount) {
                tableActor ! TableInsertRowMessage(i, List(randRang(i), "Some Other Movie"), testActor)
            }
            val t4 = System.nanoTime()
            println(s"Elapsed time (Inserting start): ${(t4 - t0)/1000000000.0}s")
            receiveN(msgCount, 2000.seconds)
            val t1 = System.nanoTime()
            println(s"Elapsed time (Inserting): ${(t1 - t0)/1000000000.0}s")

            /*
            tableActor ! "Rebalance"
            tableActor ! TableInsertRowMessage(10000003, List(1000003, "Some Other Movie"), testActor)
            receiveN(1, 2000.seconds)
            */
            val t2 = System.nanoTime()
            println(s"Elapsed time (Rebalancing): ${(t2 - t1)/1000000000.0}s")

            var queryId = msgCount + 5

            for (i <- 1 to msgCount) {
                tableActor ! TableSelectWhereMessage(msgCount+i, List("id", "title"), EqOperator("id", i), testActor)
            }
            for (i <- 1 to msgCount) {
                tableActor ! TableSelectWhereMessage(2*msgCount+i, List("id", "title"), EqOperator("id", i), testActor)
            }
            for (i <- 1 to msgCount) {
                tableActor ! TableSelectWhereMessage(3*msgCount+i, List("id", "title"), EqOperator("id", i), testActor)
            }
            for (i <- 1 to msgCount) {
                tableActor ! TableSelectWhereMessage(4*msgCount+i, List("id", "title"), EqOperator("id", i), testActor)
            }
            for (i <- 1 to msgCount) {
                tableActor ! TableSelectWhereMessage(5*msgCount+i, List("id", "title"), EqOperator("id", i), testActor)
            }
            receiveN(4*msgCount, 2000.seconds)
            val t3 = System.nanoTime()
            println(s"Elapsed time (Reading all 4 times): ${(t3 - t2)/1000000000.0}s")
            receiveN(msgCount, 2000.seconds)
            val t5 = System.nanoTime()
            println(s"Elapsed time (Reading all 5 times): ${(t5 - t2)/1000000000.0}s")

            //tableActor ! ShutdownMessage
            Thread.sleep(10)
        }

        enterBarrier("finished")
    }
}
