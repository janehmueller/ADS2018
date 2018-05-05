package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}
import de.hpi.ads.database.{Row, Table}
import de.hpi.ads.remote.actors.UserActor.RowInsertSuccessMessage
import de.hpi.ads.remote.messages.QueryResultMessage

object TableActor {
    def props(table: String, fileName: String, schemaString: String): Props = {
        Props(new TableActor(table, fileName, schemaString))
    }

    case class TableSelectByKeyMessage(key: String, queryReceiver: ActorRef)

    case class TableSelectWhereMessage(
        projection: List[String],
        conditions: Row => Boolean,
        queryReceiver: ActorRef
    )

    case class TableInsertRowMessage(data: List[String], queryReceiver: ActorRef)
}

class TableActor(table: String, fileName: String, schemaString: String)
    extends Table(fileName, schemaString) with ADSActor
{
    import TableActor._

    def receive: Receive = {
        case TableSelectByKeyMessage(key, queryReceiver) =>
            val row = this.select(key)
            queryReceiver ! QueryResultMessage(row.toList)
        case msg: TableSelectWhereMessage => log.info("received select where")
        case TableInsertRowMessage(data, queryReceiver) =>
            this.insertList(data)
            log.debug(s"Inserted row: ${data.mkString("; ")}")
            queryReceiver ! RowInsertSuccessMessage
        case default => log.error(s"Received unknown message: $default")
    }
}


