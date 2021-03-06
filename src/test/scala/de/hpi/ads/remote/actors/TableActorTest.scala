package de.hpi.ads.remote.actors

import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.testkit.{ImplicitSender, TestKit}
import de.hpi.ads.database.operators.EqOperator
import de.hpi.ads.database.types._
import de.hpi.ads.remote.messages._
//import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class TableActorTest extends TestKit(ActorSystem("TableActorTest")) with ImplicitSender
    with FlatSpecLike with Matchers with BeforeAndAfterAll
{
    var counter: Int = 0
    def tableName: String = {
        counter += 1
        s"tableActorTest_$counter"
    }

    system.eventStream.setLogLevel(Logging.WarningLevel)

    override def afterAll: Unit = {
        TestKit.shutdownActorSystem(system)
        //FileUtils.cleanDirectory(new File(TablePartitionActor.path))
    }

    "Table Actor" should "insert values" in {
        val schema = TableSchema("id:int;title:string(255)")
        val row = List(1, "Great Movie")
        val tableActor = system.actorOf(TableActor.props(tableName, schema))
        tableActor ! TableInsertRowMessage(1, row, testActor)
        expectMsg(QuerySuccessMessage(1))
        tableActor ! ShutdownMessage
    }

    it should "return inserted values" in {
        val schema = TableSchema("id:int;title:string(255)")
        val row = List(1, "Great Movie")
        val tableActor = system.actorOf(TableActor.props(tableName, schema))
        tableActor ! TableInsertRowMessage(1, row, testActor)
        expectMsg(QuerySuccessMessage(1))
        tableActor ! TableSelectWhereMessage(2, List("id", "title"), EqOperator("id", 1), testActor)
        val response = expectMsgType[QueryResultMessage]
        response.queryID shouldBe 2
        response.result should have length 1
        response.result shouldEqual List(row)
        tableActor ! ShutdownMessage
    }

    it should "select correct values with condition" in {
        val schema = TableSchema("id:int;title:string(255);year:int")
        val row1 = List(1, "Movie1", 2000)
        val row2 = List(2, "Movie2", 2001)
        val row3 = List(3, "Movie3", 2001)
        val row4 = List(4, "Movie4", 2000)
        val tableActor = system.actorOf(TableActor.props(tableName, schema))
        tableActor ! TableInsertRowMessage(1, row1, testActor)
        expectMsg(QuerySuccessMessage(1))
        tableActor ! TableInsertRowMessage(2, row2, testActor)
        expectMsg(QuerySuccessMessage(2))
        tableActor ! TableInsertRowMessage(3, row3, testActor)
        expectMsg(QuerySuccessMessage(3))
        tableActor ! TableInsertRowMessage(4, row4, testActor)
        expectMsg(QuerySuccessMessage(4))
        tableActor ! TableSelectWhereMessage(5, List("title"), EqOperator("year", 2001), testActor)
        val response = expectMsgType[QueryResultMessage]
        response.result should have length 2
        response.result shouldEqual List(List("Movie2"), List("Movie3"))
        tableActor ! ShutdownMessage
    }
}
