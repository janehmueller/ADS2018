package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}
import de.hpi.ads.database.types.{ColumnType, TableSchema}
import de.hpi.ads.remote.actors.ResultCollectorActor.PrepareNewQueryResultsMessage
import de.hpi.ads.remote.messages._

object TableActor {
    def actorName(tableName: String): String = s"TABLE_${tableName.toUpperCase}"

    def fileName(tableName: String): String = s"table.$tableName.ads"

    def props(table: String, schema: String, resultCollector: ActorRef): Props = {
        Props(new TableActor(table, TableSchema(schema), resultCollector))
    }

    def props(table: String, columns: List[ColumnType], resultCollector: ActorRef): Props = {
        Props(new TableActor(table, TableSchema(columns), resultCollector))
    }

    def props(table: String, columns: TableSchema, resultCollector: ActorRef): Props = {
        Props(new TableActor(table, columns, resultCollector))
    }
}

class TableActor(tableName: String, schema: TableSchema, resultCollector: ActorRef) extends ADSActor {
    import TableActor._

    // TODO: partition file names
    val tablePartitionActor: ActorRef = context.actorOf(
        TablePartitionActor.props(tableName, fileName(tableName), schema, resultCollector))

    override def postStop(): Unit = {
        super.postStop()
        // TODO: stop child actors (maybe via Poison Pill)
    }

    def receive: Receive = {
        /** Table Insert */
        case msg: TableInsertRowMessage => tablePartitionActor ! msg
        case msg: TableNamedInsertRowMessage => tablePartitionActor ! msg

        /** Table Read */
        case msg: TableSelectWhereMessage =>
            resultCollector ! PrepareNewQueryResultsMessage(msg.queryID, msg.receiver)
            tablePartitionActor ! msg

        /** Table Update */
        case msg: TableUpdateWhereMessage => tablePartitionActor ! msg

        /** Table Delete */
        case msg: TableDeleteWhereMessage => tablePartitionActor ! msg

        /** Handle dropping the table. */
        case ShutdownMessage =>
            tablePartitionActor ! ShutdownMessage
            context.stop(this.self)

        /** Default case */
        case default => log.error(s"Received unknown message: $default")
    }
}
