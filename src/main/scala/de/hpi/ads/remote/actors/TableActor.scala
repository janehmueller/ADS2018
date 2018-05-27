package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, PoisonPill, Props}
import de.hpi.ads.database.types.{ColumnType, TableSchema}
import de.hpi.ads.remote.actors.ResultCollectorActor.PrepareNewQueryResultsMessage
import scala.collection.mutable.{Map => MMap}

import de.hpi.ads.remote.messages._

object TableActor {
    def actorName(tableName: String): String = s"TABLE_${tableName.toUpperCase}"

    def fileName(tableName: String): String = s"table.$tableName"

    def props(table: String, schema: String, resultCollector: ActorRef): Props = {
        Props(new TableActor(table, TableSchema(schema), resultCollector))
    }

    def props(table: String, columns: List[ColumnType], resultCollector: ActorRef): Props = {
        Props(new TableActor(table, TableSchema(columns), resultCollector))
    }

    def props(table: String, columns: TableSchema, resultCollector: ActorRef): Props = {
        Props(new TableActor(table, columns, resultCollector))
    }

    case class TablePartitionStartedMessage(fileName: String, lowerBound: Any, upperBound: Any, middle: Any)
    case class TablePartitionReadyMessage(fileName: String, lowerBound: Any, upperBound: Any)
    case class InsertionDoneMessage(queryID: Int)
}

class TableActor(tableName: String, schema: TableSchema, resultCollector: ActorRef) extends ADSActor {
    import TableActor._

    // TODO: partition file names
    var tablePartitionActor: ActorRef = context.actorOf(
        TablePartitionActor.props(tableName, fileName(tableName), schema, self, resultCollector))
    var currentInsertions: Int = 0
    var currentPartitionings: Int = 0
    var currentlyRebalancing: Boolean = false
    var partitionCollection: MMap[(Any, Any), String] = MMap[(Any, Any), String]() + ((None, None) -> fileName(tableName))

    override def postStop(): Unit = {
        super.postStop()
        tablePartitionActor ! PoisonPill
    }

    def receive: Receive = {
        /** Table Insert */
        case msg: TableInsertRowMessage =>
            if (!currentlyRebalancing) {
                tablePartitionActor ! msg
                currentInsertions += 1
            } else {
                self ! msg
            }
        case msg: TableNamedInsertRowMessage =>
            if (!currentlyRebalancing) {
                tablePartitionActor ! msg
                currentInsertions += 1
            } else {
                self ! msg
            }

        /** Table Read */
        case msg: TableSelectWhereMessage =>
            resultCollector ! PrepareNewQueryResultsMessage(msg.queryID, msg.receiver)
            tablePartitionActor ! msg

        /** Table Update */
        case msg: TableUpdateWhereMessage => tablePartitionActor ! msg

        /** Table Delete */
        case msg: TableDeleteWhereMessage => tablePartitionActor ! msg
        case TablePartitionStartedMessage(fileName, lB, uB, m) => partitionStarted(fileName, lB, uB, m)
        case TablePartitionReadyMessage(fileName, lB, uB) => partitionIsReady(fileName, lB, uB)
        case InsertionDoneMessage(queryID) =>
            currentInsertions -= 1
            if (currentlyRebalancing && currentInsertions + currentPartitionings == 0) {
                rebalance()
            }

        /** Handle dropping the table. */
        case ShutdownMessage =>
            tablePartitionActor ! ShutdownMessage
            context.stop(this.self)

        /** Default case */
        case default => log.error(s"Received unknown message: $default")
    }

    def partitionStarted(fileName: String, lowerBound: Any, upperBound: Any, middle: Any): Unit = {
        currentPartitionings += 2
        partitionCollection -= ((lowerBound, upperBound) -> fileName)
    }

    def partitionIsReady(fileName: String, lowerBound: Any, upperBound: Any): Unit = {
        currentPartitionings -= 1
        partitionCollection += ((lowerBound, upperBound) -> fileName)
    }

    def startRebalancing(): Unit = {
        currentlyRebalancing = true
        if (currentInsertions + currentPartitionings == 0) {
            rebalance()
        }
    }

    def rebalance(): Unit = {
        tablePartitionActor ! PoisonPill
        // build balanced tree off of partitionCollection
        // build new topPartitionActor who gets entire tree and builds children recursively
        currentlyRebalancing = false
    }
}
