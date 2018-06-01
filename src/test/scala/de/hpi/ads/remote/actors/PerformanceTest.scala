package de.hpi.ads.remote.actors

import java.io.File
import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.event.Logging
import akka.testkit.{ImplicitSender, TestKit}
import de.hpi.ads.database.operators.EqOperator
import de.hpi.ads.database.types._

import scala.concurrent.duration.DurationInt
import de.hpi.ads.remote.messages._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}

class PerformanceTest extends TestKit(ActorSystem("TableActorTest")) with ImplicitSender
    with FlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
{
    val tableName = "tablePerformanceTest"
    val tableFileName = s"$tableName.table.ads"

    system.eventStream.setLogLevel(Logging.WarningLevel)

    override def afterAll: Unit = {
        TestKit.shutdownActorSystem(system)
        Files.deleteIfExists(Paths.get(tableFileName))
    }

    it should "return inserted values" in {
        val schema = TableSchema("id:int;title:string(255)")
        val row = List(1, "Great Movie")
        val tableActor = system.actorOf(TableActor.props(tableName, schema))

        val t0 = System.nanoTime()
        tableActor ! TableInsertRowMessage(1, row, testActor)
        for (i <- 2 to 1000000) {
            tableActor ! TableInsertRowMessage(i, List(i, "Some Other Movie"), testActor)
        }
        val t4 = System.nanoTime()
        println(s"Elapsed time (Inserting start): ${(t4 - t0)/1000000000.0}s")
        for (i <- 1 to 1000000) {
            expectMsg(QuerySuccessMessage(i))
        }
        val t1 = System.nanoTime()
        println(s"Elapsed time (Inserting): ${(t1 - t0)/1000000000.0}s")

        val someFile = new File(tableFileName)
        val fileSize = someFile.length
        println("File size: " + fileSize)
        val tS = System.nanoTime()
        val byteArray = Files.readAllBytes(Paths.get(tableFileName))
        val tE = System.nanoTime()
        println(s"Elapsed time (Simple read): ${(tE - tS)/1000000000.0}s")

        tableActor ! TableSelectWhereMessage(1000001, List("id", "title"), EqOperator("id", 1), testActor)
        val response = expectMsgType[QueryResultMessage](20.seconds)
        val t2 = System.nanoTime()
        println(s"Elapsed time (Reading): ${(t2 - tE)/1000000000.0}s")

        response.queryID shouldBe 1000001
        response.result should have length 1
        response.result shouldEqual List(row)
        tableActor ! ShutdownMessage
    }

}
