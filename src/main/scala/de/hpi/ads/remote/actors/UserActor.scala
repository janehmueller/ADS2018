package de.hpi.ads.remote.actors

import java.util.UUID

import akka.actor.{ActorRef, Props}
import de.hpi.ads.remote.actors.InterfaceActor.{CreateTableMessage, InsertRowMessage, SelectWhereMessage}
import de.hpi.ads.remote.messages.{QueryFailedMessage, QueryResultMessage}

object UserActor {
    val defaultName = "USER"

    def props(interfaceActor: ActorRef): Props = Props(new UserActor(interfaceActor))

    // user interaction
    case class UserCreateTableMessage(table: String, columnNames: List[String], columnDataTypes: List[Any], columnSizes: List[Int])
    case class UserInsertValuesMessage(table: String, values: List[Any])
    case class UserSelectValuesMessage(table: String,
                                       projection: List[String],
                                       conditionColumnNames: List[String],
                                       conditionOperators: List[String],
                                       conditionValues: List[Any])

    // system interaction
    case class TableCreationSuccessMessage(table: String)
    case object RowInsertSuccessMessage
}

class UserActor(interfaceActor: ActorRef) extends ADSActor {
    import UserActor._

    def receive: Receive = {
        // user messages
        case UserCreateTableMessage(table, columnNames, columnDataTypes, columnSizes) => createTable(table, columnNames, columnDataTypes, columnSizes)
        case UserInsertValuesMessage(table, values) => insertValues(table, values)
        case UserSelectValuesMessage(table, projection, conditionColumnNames, conditionOperators, conditionValues) => selectValues(table, projection, conditionColumnNames, conditionOperators, conditionValues)

        // system messages
        case QueryResultMessage(queryId, result) =>
            println(s"Results for query $queryId: ${result.map(_.toList).mkString("\n")}")
        case TableCreationSuccessMessage(table) => log.info(s"Successfully created table $table")
        case RowInsertSuccessMessage => log.info("Successfully inserted row.")
        case QueryFailedMessage(queryID, message) => log.error(s"Query $queryID failed: $message")
        case default => log.error(s"Received unknown message: $default")
    }

    def createTable(table: String, columnNames: List[String], columnDataTypes: List[Any], columnSizes: List[Int]): Unit = {
        interfaceActor ! CreateTableMessage(table, columnNames, columnDataTypes, columnSizes)
    }

    def insertValues(table: String, values: List[Any]): Unit = {
        interfaceActor ! InsertRowMessage(table, values)
    }

    def selectValues(table: String,
                     projection: List[String],
                     conditionColumnNames: List[String],
                     conditionOperators: List[String],
                     conditionValues: List[Any]): Unit = {
        interfaceActor ! SelectWhereMessage(table, projection, conditionColumnNames, conditionOperators, conditionValues)
    }
}
