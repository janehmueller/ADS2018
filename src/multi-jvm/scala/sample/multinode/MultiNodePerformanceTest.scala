package sample.multinode

// config
import akka.cluster.Cluster
import akka.cluster.MemberStatus.Up
import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.ConfigFactory
import de.hpi.ads.database.types.TableSchema
import de.hpi.ads.remote.actors.TableActor
import de.hpi.ads.remote.messages._
import de.hpi.ads.database.operators._

import scala.concurrent.duration.DurationInt

object MultiNodePerformanceTestConfig extends MultiNodeConfig {
    commonConfig(ConfigFactory.parseString("akka.cluster.auto-join = off\n" +
        "akka.actor.provider = cluster\n" +
        "ads.hierarchyMode = flat\n" +
        "ads.maxChildren = 5\n" +
        "akka.actor.warn-about-java-serializer-usage = false"))
    val node1 = role("node1")
    val node2 = role("node2")
}
// config

// spec
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import akka.actor.{ Props, Actor }

class MultiNodePerformanceTestSpecMultiJvmNode1 extends MultiNodePerformanceTest
class MultiNodePerformanceTestSpecMultiJvmNode2 extends MultiNodePerformanceTest

object MultiNodePerformanceTest {
}

class MultiNodePerformanceTest extends MultiNodeSpec(MultiNodePerformanceTestConfig)
    with STMultiNodeSpec with ImplicitSender {

    import MultiNodePerformanceTestConfig._
    import MultiNodePerformanceTest._

    var counter: Int = 0
    def tableName: String = {
        counter += 1
        s"tablePerformanceTest_$counter"
    }
    def initialParticipants = roles.size

    val firstAddress = node(node1).address
    val secondAddress = node(node2).address

    "A MultiNodePerformanceTest" must {

        "wait for all nodes to enter a barrier" in {
            enterBarrier("startup")
        }

        "illustrate how to start up first node" in {

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

        "illustrate join more nodes" in within(10 seconds) {
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

        "send to and receive from a remote node" in {
            runOn(node1) {
                val schema = TableSchema("id:int;title:string(20)")
                val row = List(1, "Great Movie")
                val tableActor = system.actorOf(TableActor.props(tableName, schema))
                Thread.sleep(1000)
                val t0 = System.nanoTime()
                tableActor ! TableInsertRowMessage(1, row, testActor)
                val msgCount = 21
                for (i <- 2 to msgCount) {
                    tableActor ! TableInsertRowMessage(i, List(i, "Some Other Movie"), testActor)
                }
                val t4 = System.nanoTime()
                println(s"Elapsed time (Inserting start): ${(t4 - t0)/1000000000.0}s")
                receiveN(msgCount, 2000.seconds)
                val t1 = System.nanoTime()
                println(s"Elapsed time (Inserting): ${(t1 - t0)/1000000000.0}s")
                tableActor ! "Rebalance"
                tableActor ! TableInsertRowMessage(1000003, List(1000003, "Some Other Movie"), testActor)
                receiveN(1, 2000.seconds)
                val t2 = System.nanoTime()
                println(s"Elapsed time (Rebalancing): ${(t2 - t1)/1000000000.0}s")

                var queryId = msgCount + 5
                tableActor ! TableSelectWhereMessage(queryId, List("id", "title"), EqOperator("id", 1), testActor)
                var response = expectMsgType[QueryResultMessage]
                val t3 = System.nanoTime()
                println(s"Elapsed time (Reading EqOperator): ${(t3 - t2)/1000000000.0}s")
                response.queryID shouldBe queryId
                response.result should have length 1

                //tableActor ! ShutdownMessage
                Thread.sleep(10)
            }

            enterBarrier("finished")
        }
    }
}
// spec
