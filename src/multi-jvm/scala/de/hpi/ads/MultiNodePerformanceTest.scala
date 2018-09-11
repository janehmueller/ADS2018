package de.hpi.ads

import akka.actor.Address
import de.hpi.ads.database.types.TableSchema
import de.hpi.ads.remote.actors.TableActor
import de.hpi.ads.remote.messages._
import de.hpi.ads.database.operators._
import de.hpi.ads.specs.{MultiNodeADSConfig, STMultiNodeSpec}

import scala.concurrent.duration.DurationInt

import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender

class MultiNodePerformanceTestMultiJvmNode1 extends MultiNodePerformanceTest
class MultiNodePerformanceTestMultiJvmNode2 extends MultiNodePerformanceTest
//class MultiNodePerformanceTestMultiJvmNode3 extends MultiNodePerformanceTest
//class MultiNodePerformanceTestMultiJvmNode4 extends MultiNodePerformanceTest

class MultiNodePerformanceTest extends MultiNodeSpec(MultiNodeADSConfig) with STMultiNodeSpec with ImplicitSender {

    import MultiNodeADSConfig._

    var counter: Int = 0
    def tableName: String = {
        counter += 1
        s"tablePerformanceTest_$counter"
    }
    def initialParticipants: Int = roles.size

    val firstAddress: Address = node(node1).address
    val secondAddress: Address = node(node2).address

    "MultiNode Table" should "insert and return values" in {
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
