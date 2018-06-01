package de.hpi.ads

import akka.actor.{ActorRef, ActorSystem}
import de.hpi.ads.database.operators.{EqOperator, LessThanOperator}
import de.hpi.ads.database.types._
import de.hpi.ads.remote.actors.InterfaceActor._
import de.hpi.ads.remote.actors.UserActor.ExecuteCommandMessage
import de.hpi.ads.remote.actors._

object Main {
    def main(args: Array[String]): Unit = {
        val actorSystem = ActorSystem("ActorDatabaseSystem")
        val interfaceActor = actorSystem.actorOf(InterfaceActor.props, InterfaceActor.defaultName)
        val userActor = actorSystem.actorOf(UserActor.props(interfaceActor), UserActor.defaultName)
        userActor ! ExecuteCommandMessage(CreateTableMessage("actors", "id:string(255);name:string(255);surname:string(255)"))
        userActor ! ExecuteCommandMessage(InsertRowMessage("actors", List("1", "Max", "Mustermann")))
        userActor ! ExecuteCommandMessage(InsertRowMessage("actors", List("2", "Max", "Metermann")))
        userActor ! ExecuteCommandMessage(SelectWhereMessage("actors", List("id", "name", "surname"), EqOperator("name", "Max")))
        val movieTypes = IndexedSeq(
            ColumnType("id", IntType),
            ColumnType("name", StringType()),
            ColumnType("rating", DoubleType)
        )
        userActor ! ExecuteCommandMessage(CreateTableWithTypesMessage("movies", movieTypes))
        userActor ! ExecuteCommandMessage(NamedInsertRowMessage("movies", List(("id", 1), ("name", "Ready"), ("rating", 3.5))))
        userActor ! ExecuteCommandMessage(NamedInsertRowMessage("movies", List(("id", 2), ("name", "Player"), ("rating", 4.5))))
        userActor ! ExecuteCommandMessage(NamedInsertRowMessage("movies", List(("id", 3), ("name", "One"), ("rating", 5.5))))
        userActor ! ExecuteCommandMessage(SelectWhereMessage("movies", List("id", "name", "rating"), EqOperator("id", 1)))

        userActor ! ExecuteCommandMessage(UpdateWhereMessage("movies", List(("name", "Ready Player One"), ("rating", 9.5)), EqOperator("id", 1)))
        userActor ! ExecuteCommandMessage(SelectWhereMessage("movies", List("id", "name", "rating"), LessThanOperator("id", 3)))
        while (true) {}
        actorSystem.terminate()
    }

    def startActorSystem(): ActorRef = {
        val actorSystem = ActorSystem("ActorDatabaseSystem")
        val interfaceActor = actorSystem.actorOf(InterfaceActor.props, InterfaceActor.defaultName)
        val userActor = actorSystem.actorOf(UserActor.props(interfaceActor), UserActor.defaultName)
        userActor
    }
}
