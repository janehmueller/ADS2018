package de.hpi.ads.remote.actors

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import de.hpi.ads.remote.actors.UserActor.RowInsertSuccessMessage
import de.hpi.ads.remote.messages.QueryResultMessage
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class TableActorTest extends TestKit(ActorSystem("TableActorTest")) with ImplicitSender
    with FlatSpecLike with Matchers with BeforeAndAfterAll
{
    val tableFileName = "tableActorTest.ads"
    val tableFileFullPath: String = s"src/test/resources/$tableFileName"

    override def afterAll: Unit = {
        TestKit.shutdownActorSystem(system)
    }

    "Table Actor" should "insert values" in {
        val schema = "id;title"
        val row = List("1", "Great Movie")
        val tableActor = system.actorOf(TableActor.props("test", tableFileFullPath, schema))
        tableActor ! TableActor.TableInsertRowMessage(row, testActor)
        expectMsg(RowInsertSuccessMessage)
    }

    it should "return inserted values" in {
        val schema = "id;title"
        val row = List("1", "Great Movie")
        val tableActor = system.actorOf(TableActor.props("test", tableFileFullPath, schema))
        tableActor ! TableActor.TableInsertRowMessage(row, testActor)
        expectMsg(RowInsertSuccessMessage)
        tableActor ! TableActor.TableSelectByKeyMessage(1, "1", testActor)
        val response = expectMsgType[QueryResultMessage]
        response.queryID shouldBe 1
        response.result should have length 1
        response.result.head.toList shouldEqual row
    }
}
