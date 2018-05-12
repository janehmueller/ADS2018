package de.hpi.ads.remote.actors

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import de.hpi.ads.database.Table
import de.hpi.ads.database.types.TableSchema
import de.hpi.ads.remote.actors.UserActor.RowInsertSuccessMessage
import de.hpi.ads.remote.messages.QueryResultMessage
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}

class TableActorTest extends TestKit(ActorSystem("TableActorTest")) with ImplicitSender
    with FlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
{
    val tableFileName = "tableActorTest.ads"
    val tableFileFullPath: String = s"src/test/resources/$tableFileName"

    override def afterAll: Unit = {
        TestKit.shutdownActorSystem(system)
    }

    override def afterEach: Unit = {
        //TODO drop table after each test
    }


    //TODO this test should probably be in some other file
    "Table" should "insert and read correctly" in {
        val schema = TableSchema(List("id", "title", "long", "double"), List(Int, "S", Long, Double), List(4, 255, 8, 8))
        val row1 = List(1, "abcde", 34927L, 5.8)
        val row2 = List(2, "fgr", 344366927L, 4.9)
        val table = Table("tableTestFile", schema)
        table.insertList(row1)
        table.insertList(row2)
        val allRows = table.readAll()
        table.cleanUp()
        allRows shouldEqual List(row1, row2)
    }

    "Table Actor" should "insert values" in {
        val schema = TableSchema(List("id", "title"), List(Int, "S"), List(4, 255))
        val row = List(1, "Great Movie")
        val tableActor = system.actorOf(TableActor.props("test", tableFileFullPath, schema))
        tableActor ! TableActor.TableInsertRowMessage(row, testActor)
        expectMsg(RowInsertSuccessMessage)

    }

    it should "return inserted values" in {
        val schema = TableSchema(List("id", "title"), List(Int, "S"), List(4, 255))
        val row = List(1, "Great Movie")
        val tableActor = system.actorOf(TableActor.props("test", tableFileFullPath, schema))
        tableActor ! TableActor.TableInsertRowMessage(row, testActor)
        expectMsg(RowInsertSuccessMessage)
        tableActor ! TableActor.TableSelectWhereMessage(1, null, List("id"), List("="), List(1), testActor)
        val response = expectMsgType[QueryResultMessage]
        response.queryID shouldBe 1
        response.result should have length 1
        response.result shouldEqual List(row)

    }

    it should "select correct values with condition" in {
        val schema = TableSchema(List("id", "title", "year"), List(Int, "S", Int), List(4, 255, 4))
        val row1 = List(1, "Movie1", 2000)
        val row2 = List(2, "Movie2", 2001)
        val row3 = List(3, "Movie3", 2001)
        val row4 = List(4, "Movie4", 2000)
        val tableActor = system.actorOf(TableActor.props("test", tableFileFullPath, schema))
        tableActor ! TableActor.TableInsertRowMessage(row1, testActor)
        expectMsg(RowInsertSuccessMessage)
        tableActor ! TableActor.TableInsertRowMessage(row2, testActor)
        expectMsg(RowInsertSuccessMessage)
        tableActor ! TableActor.TableInsertRowMessage(row3, testActor)
        expectMsg(RowInsertSuccessMessage)
        tableActor ! TableActor.TableInsertRowMessage(row4, testActor)
        expectMsg(RowInsertSuccessMessage)
        tableActor ! TableActor.TableSelectWhereMessage(1, List("title"), List("year"), List("="), List(2001), testActor)
        val response = expectMsgType[QueryResultMessage]
        response.result should have length 2
        response.result shouldEqual List(List("Movie2"), List("Movie3"))

    }
}
