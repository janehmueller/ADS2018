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

//    case class SelectWhereMessage(queryID: Int, projection: List[String], conditionColumnNames: List[String], conditionOperators: List[String], conditionValues: List[Any], receiver: ActorRef)
//    def selectWhere(queryID: Int, projection: List[String], conditionColumnNames: List[String], conditionOperators: List[String], conditionValues: List[Any], receiver: ActorRef): Unit = {
//        //gather answer either from child actors or from associated data
//        //TODO check if index exists and use it
//        if (children.nonEmpty) {
//            receiver ! ExpectResultsMessage(queryID, children.size - 1)
//            children.foreach(child => child ! SelectWhereMessage(queryID, projection, conditionColumnNames, conditionOperators, conditionValues, receiver))
//        } else {
//            val result: ListBuffer[List[Any]] = ListBuffer()
//            if (conditionColumnNames.size == 1) {
//                val conditionColumn: Int = schema.columnIndex(conditionColumnNames(0))
//                if (conditionOperators(0) == "=") {
//                    result ++= this.selectWhere(_(conditionColumn) == conditionValues(0))
//                } else if (conditionOperators(0) == "<=" && schema.columnDataTypes(conditionColumn) == Int) {
//                    result ++= this.selectWhere(_(conditionColumn).asInstanceOf[Int] <= conditionValues(0).asInstanceOf[Int])
//                } else if (conditionOperators(0) == ">=" && schema.columnDataTypes(conditionColumn) == Int) {
//                    result ++= this.selectWhere(_(conditionColumn).asInstanceOf[Int] >= conditionValues(0).asInstanceOf[Int])
//                } else {
//                    assert(false, "Operation not supported")
//                }
//            } else if (conditionColumnNames.size == 2) {
//                val conditionColumn1: Int = schema.columnIndex(conditionColumnNames(0))
//                if (conditionColumnNames(0) == conditionColumnNames(1) && conditionOperators(0) == "<=" && conditionOperators(1) == ">=" && schema.columnDataTypes(conditionColumn1) == Int) {
//                    result ++= this.selectWhere(x => x(conditionColumn1).asInstanceOf[Int] <= conditionValues(0).asInstanceOf[Int] && x(conditionColumn1).asInstanceOf[Int] >= conditionValues(1).asInstanceOf[Int])
//                } else {
//                    assert(false, "Operation not supported")
//                }
//            } else {
//                assert(false, "Operation not supported")
//            }
//            if (projection != null) {
//                receiver ! QueryResultMessage(queryID, this.projectRows(projection, result.toList))
//            } else {
//                receiver ! QueryResultMessage(queryID, result.toList)
//            }
//        }
//    }
