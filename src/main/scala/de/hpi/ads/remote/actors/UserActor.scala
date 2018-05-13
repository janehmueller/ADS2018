package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}
import de.hpi.ads.remote.messages.{QueryFailureMessage, QueryResultMessage, QuerySuccessMessage}

object UserActor {
    val defaultName = "USER"

    def props(interfaceActor: ActorRef): Props = Props(new UserActor(interfaceActor))

    /** User interaction */
    case class ExecuteCommandMessage(command: Product)

    /** System responses */
    case class TableOpSuccessMessage(table: String, op: String)
    case class TableOpFailureMessage(table: String, op: String, message: String)
    case object RowInsertSuccessMessage
}

class UserActor(interfaceActor: ActorRef) extends ADSActor {
    import UserActor._

    def receive: Receive = {
        /** Execute user commands */
        case ExecuteCommandMessage(command) => interfaceActor ! command

        /** Table operation status responses */
        case TableOpSuccessMessage(table, op) => log.info(s"Successfully finished operation $op on table $table")
        case TableOpFailureMessage(table, op, message) => log.error(s"Operation $op failed on table $table: $message")

        /** Query Results */
        case QueryResultMessage(queryID, result) =>
            println(s"Results for query $queryID:\n${result.map(_.toList).mkString("\n")}")
        case QuerySuccessMessage(queryID) => log.info(s"Query $queryID succeeded")
        case QueryFailureMessage(queryID, message) => log.error(s"Query $queryID failed: $message")

        /** Default case */
        case default => log.error(s"Received unknown message: $default")
    }
}
