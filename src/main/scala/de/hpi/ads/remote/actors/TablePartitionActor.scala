package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}
import de.hpi.ads.database.{Row, Table}
import de.hpi.ads.database.types.{ColumnType, TableSchema}
import de.hpi.ads.remote.actors.ResultCollectorActor.{ExpectResultsMessage, PrepareNewQueryResultsMessage}
import de.hpi.ads.remote.actors.UserActor.TableOpFailureMessage
import de.hpi.ads.remote.messages._

import scala.util.Random

object TablePartitionActor {
    def actorName(tableName: String): String = s"TABLE_${tableName.toUpperCase}"

    def fileName(tableName: String): String = s"table.$tableName.ads"

    def props(table: String, fileName: String, schema: TableSchema, resultCollector: ActorRef): Props = {
        Props(new TablePartitionActor(table, fileName, schema, resultCollector))
    }
}

class TablePartitionActor(tableName: String, fileName: String, schema: TableSchema, resultCollector: ActorRef)
    extends Table(fileName, schema) with ADSActor
{
    import TablePartitionActor._

    val children : List[ActorRef] = List()
    val RNG = new Random()

    override def postStop(): Unit = {
        super.postStop()
        this.cleanUp()
        // TODO: stop child actors (maybe via Poison Pill)
    }

    def receive: Receive = {
        /** Table Insert */
        case TableInsertRowMessage(queryID, data, receiver) => insertRow(queryID, data, receiver)
        case TableNamedInsertRowMessage(queryID, data, receiver) => insertRowWithNames(queryID, data, receiver)

        /** Table Read */
        case TableSelectWhereMessage(queryID, projection, conditions, receiver) =>
            resultCollector ! PrepareNewQueryResultsMessage(queryID, receiver)
            selectWhere(queryID, projection, conditions, resultCollector)

        /** Table Update */
        case TableUpdateWhereMessage(queryID, data, conditions, receiver) =>
            updateWhere(queryID, data, conditions, receiver)

        /** Table Delete */
        case TableDeleteWhereMessage(queryID, conditions, receiver) =>
            deleteWhere(queryID, conditions, receiver)

        /** Handle dropping the table. */
        case ShutdownMessage =>
            // TODO stop children
            context.stop(this.self)

        /** Default case */
        case default => log.error(s"Received unknown message: $default")
    }

    def insertRow(queryID: Int, data: List[Any], receiver: ActorRef): Unit = {
        if (children.nonEmpty) {
            // TODO: figure out which child actor should receive the row according to splitting
            // for now: give it to random child
            children(RNG.nextInt(children.size)) ! TableInsertRowMessage(queryID, data, receiver)
        } else {
            if (!inputContainsValidKey(schema.columnNames.zip(data))) {
                receiver ! TableOpFailureMessage(tableName, "INSERT", "Input does not contain valid primary key.")
                return
            }
            this.insertList(data)
            receiver ! QuerySuccessMessage(queryID)
        }
    }

    def insertRowWithNames(queryID: Int, data: List[(String, Any)], receiver: ActorRef): Unit = {
        if (children.nonEmpty) {
            // TODO: figure out which child actor should receive the row according to splitting
            // for now: give it to random child
            children(RNG.nextInt(children.size)) ! TableNamedInsertRowMessage(queryID, data, receiver)
        } else {
            if (!inputContainsValidKey(data)) {
                receiver ! TableOpFailureMessage(tableName, "INSERT", "Input does not contain valid primary key.")
                return
            }
            this.insert(data)
            receiver ! QuerySuccessMessage(queryID)
        }
    }

    /**
      * Validates that the input contains a primary key and that this primary key does not already exist.
      * @param data input data that is validated
      * @return true if the input data is valid
      */
    def inputContainsValidKey(data: List[(String, Any)]): Boolean = {
        val keyExistsOption = data.find(_._1 == schema.keyColumn)
        val duplicateKeyOption = keyExistsOption.filter { case (columnName, key) => this.keyPositions.contains(key) }
        duplicateKeyOption.isEmpty
    }

    def selectWhere(queryID: Int, projection: List[String], conditions: Row => Boolean, receiver: ActorRef): Unit = {
        // gather answers either from child actors or from associated data
        // TODO check if index exists and use it
        if (children.nonEmpty) {
            receiver ! ExpectResultsMessage(queryID, children.length - 1)
            children.foreach(_ ! TableSelectWhereMessage(queryID, projection, conditions, receiver))
        }
        val result = this.selectWhere(conditions)
        // TODO projection * (select *) operator
        val projectedResult = result.map(_.project(projection))
        receiver ! QueryResultMessage(queryID, projectedResult)
    }

    def updateWhere(queryID: Int, data: List[(String, Any)], conditions: Row => Boolean, receiver: ActorRef): Unit = {
        if (children.nonEmpty) {
            // TODO: figure out which child actors store the relevant rows
            // for now: give it to random child
            children(RNG.nextInt(children.size)) ! TableUpdateWhereMessage(queryID, data, conditions, receiver)
        } else {
            this.updateWhere(data, conditions)
            receiver ! QuerySuccessMessage(queryID)
        }
    }

    def deleteWhere(queryID: Int, conditions: Row => Boolean, receiver: ActorRef): Unit = {
        // TODO: figure out how to delete data from file without breaking reading the file
        // TODO: (e.g. write a Boolean in the row "header" that indicates if this row was deleted)
        if (children.nonEmpty) {
            // TODO: figure out which child actors store the relevant rows
            // for now: give it to random child
            children(RNG.nextInt(children.size)) ! TableDeleteWhereMessage(queryID, conditions, receiver)
        } else {
            this.deleteWhere(conditions)
            receiver ! QuerySuccessMessage(queryID)
        }
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
