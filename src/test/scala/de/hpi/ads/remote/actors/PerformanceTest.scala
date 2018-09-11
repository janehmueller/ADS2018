package de.hpi.ads.remote.actors

import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.testkit.{ImplicitSender, TestKit}
import de.hpi.ads.database.operators.{EqOperator, LessThanOperator}
import de.hpi.ads.database.types._
import de.hpi.ads.remote.actors.TableActor.TableExpectDenseInsertRange

import scala.concurrent.duration.DurationInt
import de.hpi.ads.remote.messages._
//import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}

class PerformanceTest extends TestKit(ActorSystem("PerformanceTest")) with ImplicitSender
    with FlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
{
    var counter: Int = 0
    def tableName: String = {
        counter += 1
        s"tablePerformanceTest_$counter"
    }

    system.eventStream.setLogLevel(Logging.WarningLevel)

    override def afterAll: Unit = {
        TestKit.shutdownActorSystem(system)
        //FileUtils.cleanDirectory(new File(TablePartitionActor.path))
    }

    "Table Partition Actor" should "rebalance quickly" in {
        val schema = TableSchema("id:int;title:string(20)")
        val row = List(1, "Great Movie")
        val tableActor = system.actorOf(TableActor.props(tableName, schema))

        val t0 = System.nanoTime()
        tableActor ! TableInsertRowMessage(1, row, testActor)
        val msgCount = 100000
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
        tableActor ! ShutdownMessage
    }

    it should "be faster when preparing" in {
        val schema = TableSchema("id:int;title:string(20)")
        val row = List(1, "Great Movie")
        val tableActor = system.actorOf(TableActor.props(tableName, schema))
        val msgCount = 100000

        tableActor ! TableExpectDenseInsertRange(1, msgCount)
        val t0 = System.nanoTime()
        tableActor ! TableInsertRowMessage(1, row, testActor)

        for (i <- 2 to msgCount) {
            tableActor ! TableInsertRowMessage(i, List(i, "Some Other Movie"), testActor)
        }
        val t4 = System.nanoTime()
        println(s"Elapsed time (Inserting start): ${(t4 - t0)/1000000000.0}s")
        receiveN(msgCount, 2000.seconds)
        val t1 = System.nanoTime()
        println(s"Elapsed time (Inserting): ${(t1 - t0)/1000000000.0}s")
        tableActor ! ShutdownMessage
    }

    it should "return inserted values" in {
        val schema = TableSchema("id:int;title:string(20)")
        val row = List(1, "Great Movie")
        val tableActor = system.actorOf(TableActor.props(tableName, schema))
        val msgCount = 100000

        val t0 = System.nanoTime()
        tableActor ! TableInsertRowMessage(1, row, testActor)
        for (i <- 2 to msgCount) {
            tableActor ! TableInsertRowMessage(i, List(i, "Some Other Movie"), testActor)
        }
        val t4 = System.nanoTime()
        println(s"Elapsed time (Inserting start): ${(t4 - t0)/1000000000.0}s")
        receiveN(msgCount, 2000.seconds)
        val t1 = System.nanoTime()
        println(s"Elapsed time (Inserting): ${(t1 - t0)/1000000000.0}s")

        var queryId = msgCount + 1
        tableActor ! TableSelectWhereMessage(queryId, List("id", "title"), EqOperator("id", 1), testActor)
        var response = expectMsgType[QueryResultMessage]
        val t2 = System.nanoTime()
        println(s"Elapsed time (Reading EqOperator): ${(t2 - t1)/1000000000.0}s")
        response.queryID shouldBe queryId
        response.result should have length 1
        response.result shouldEqual List(row)

        queryId += 1
        tableActor ! TableSelectWhereMessage(queryId, List("id", "title"), LessThanOperator("id", 10), testActor)
        response = expectMsgType[QueryResultMessage]
        val t3 = System.nanoTime()
        println(s"Elapsed time (Reading LessThanOperator): ${(t3 - t2)/1000000000.0}s")
        response.queryID shouldBe queryId
        response.result should have length 9

        tableActor ! ShutdownMessage
    }
}
