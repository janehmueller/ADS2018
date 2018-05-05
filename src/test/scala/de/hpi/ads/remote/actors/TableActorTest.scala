package de.hpi.ads.actors

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class TableActorTest extends TestKit(ActorSystem("TableActorTest")) with ImplicitSender
    with FlatSpecLike with Matchers with BeforeAndAfterAll
{
    val tableFile = "tableActorTest"
    val tableFileFullPath: String = s"src/test/resources/$tableFile"

    override def afterAll: Unit = {
        TestKit.shutdownActorSystem(system)
    }



    "Table Actor" should "send back messages unchanged" in {
        val schema = "id;title"
        val tableActor = system.actorOf(TableActor.props(tableFileFullPath, schema))
        tableActor ! TableActor.InsertRowMessage(Nil, testActor)
        expectMsg("hello world")
    }
}
