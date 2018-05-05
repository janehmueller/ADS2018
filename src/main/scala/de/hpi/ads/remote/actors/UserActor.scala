package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}
import de.hpi.ads.database.Row
import de.hpi.ads.remote.actors.InterfaceActor.{CreateTableMessage, InsertRowMessage, SelectWhereMessage}
import de.hpi.ads.remote.messages.QueryResultMessage

object UserActor {
    val defaultName = "USER"

    def props(interfaceActor: ActorRef): Props = Props(new UserActor(interfaceActor))

    // user interaction
    case class UserCreateTableMessage(table: String, schema: String)
    case class UserInsertValuesMessage(table: String, values: List[Any])
    case class UserSelectValuesMessage(table: String, projection: List[String], conditions: Row => Boolean)

    // system interaction
    case class TableCreationSuccessMessage(table: String)
    case object RowInsertSuccessMessage

}

class UserActor(interfaceActor: ActorRef) extends ADSActor {
    import UserActor._

    def receive: Receive = {
        // user messages
        case UserCreateTableMessage(table, schema) => createTable(table, schema)
        case UserInsertValuesMessage(table, values) => insertValues(table, values)
        case UserSelectValuesMessage(table, projection, conditions) => selectValues(table, projection, conditions)

        // system messages
        case QueryResultMessage(result) => result.foreach { resultRow => println(resultRow.toList)}
        case TableCreationSuccessMessage(table) => log.info(s"Successfully created table $table")
        case RowInsertSuccessMessage => log.info("Successfully inserted row.")
        case default => log.error(s"Received unknown message: $default")
    }

    def createTable(table: String, schema: String): Unit = {
        interfaceActor ! CreateTableMessage(table, schema)
    }

    def insertValues(table: String, values: List[Any]): Unit = {
        interfaceActor ! InsertRowMessage(table, values)
    }

    def selectValues(table: String, projection: List[String], conditions: Row => Boolean): Unit = {
        interfaceActor ! SelectWhereMessage(table, projection, conditions)
    }
}
