package de.hpi.ads

import akka.actor.{ActorRef, ActorSystem}
import de.hpi.ads.database.types._
import de.hpi.ads.remote.actors.InterfaceActor._
import de.hpi.ads.remote.actors.UserActor.ExecuteCommandMessage
import de.hpi.ads.remote.actors._

object Main {
    def main(args: Array[String]): Unit = {
        val actorSystem = ActorSystem("ActorDatabaseSystem")
        val resultCollector = actorSystem.actorOf(ResultCollectorActor.props(), ResultCollectorActor.defaultName)
        val interfaceActor = actorSystem.actorOf(InterfaceActor.props(resultCollector), InterfaceActor.defaultName)
        val userActor = actorSystem.actorOf(UserActor.props(interfaceActor), UserActor.defaultName)
        userActor ! ExecuteCommandMessage(CreateTableMessage("actors", "id:string(255);name:string(255);surname:string(255)"))
        userActor ! ExecuteCommandMessage(InsertRowMessage("actors", List("1", "Max", "Mustermann")))
        userActor ! ExecuteCommandMessage(InsertRowMessage("actors", List("2", "Max", "Metermann")))
        userActor ! ExecuteCommandMessage(SelectWhereMessage("actors", List("id", "name", "surname"), _.name == "Max"))
        val movieTypes = List(
            ColumnType("id", IntType),
            ColumnType("name", StringType()),
            ColumnType("rating", DoubleType)
        )
        userActor ! ExecuteCommandMessage(CreateTableWithTypesMessage("movies", movieTypes))
        val row = List(
            ("id", 1),
            ("name", "Ready"),
            ("rating", 3.5)
        )
        userActor ! ExecuteCommandMessage(NamedInsertRowMessage("movies", row))
        userActor ! ExecuteCommandMessage(SelectWhereMessage("movies", List("id", "name", "rating"), _.id == 1))
        userActor ! ExecuteCommandMessage(UpdateWhereMessage("movies", List(("name", "Ready Player One"), ("rating", 9.5)), _.id == 1))
        userActor ! ExecuteCommandMessage(SelectWhereMessage("movies", List("id", "name", "rating"), _.id == 1))
        while (true) {}
        actorSystem.terminate()
    }

    def startActorSystem(): ActorRef = {
        val actorSystem = ActorSystem("ActorDatabaseSystem")
        val resultCollector = actorSystem.actorOf(ResultCollectorActor.props(), ResultCollectorActor.defaultName)
        val interfaceActor = actorSystem.actorOf(InterfaceActor.props(resultCollector), InterfaceActor.defaultName)
        val userActor = actorSystem.actorOf(UserActor.props(interfaceActor), UserActor.defaultName)
        userActor
    }
}
