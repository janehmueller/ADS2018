package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}
import de.hpi.ads.database.{Table}
import de.hpi.ads.database.types.TableSchema
import de.hpi.ads.remote.actors.ResultCollectorActor.ExpectResultsMessage
import de.hpi.ads.remote.actors.UserActor.RowInsertSuccessMessage
import de.hpi.ads.remote.messages.QueryResultMessage

import scala.collection.mutable.ListBuffer
import scala.util.Random

object RowsActor {
    def props(fileName: String, schema: TableSchema): Props = Props(new RowsActor(fileName, schema))

    case class InsertRowMessage(data: List[Any], receiver: ActorRef)

    case class SelectWhereMessage(queryID: Int, projection: List[String], conditionColumnNames: List[String], conditionOperators: List[String], conditionValues: List[Any], receiver: ActorRef)

}

class RowsActor(fileName: String, schema: TableSchema) extends Table(fileName, schema) with ADSActor {
    import RowsActor._

    val children : List[ActorRef] = List()
    val RNG = new Random()


    def receive: Receive = {
        case InsertRowMessage(data, receiver) => insertRow(data, receiver)
        case SelectWhereMessage(queryID, projection, conditionColumnNames, conditionOperators, conditionValues, receiver) =>
            selectWhere(queryID, projection, conditionColumnNames, conditionOperators, conditionValues, receiver)
        case default => log.error(s"Received unknown message: $default")
    }

    def insertRow(data: List[Any], receiver: ActorRef): Unit = {
        if (children.nonEmpty) {
            //figure out which child should receive the row according to splitting
            //for now: give it to random child TODO
            children(RNG.nextInt(children.size)) ! InsertRowMessage(data, receiver)
        } else {
            //insert row to self
            this.insertList(data)
            receiver ! RowInsertSuccessMessage
        }
    }

    def selectWhere(queryID: Int, projection: List[String], conditionColumnNames: List[String], conditionOperators: List[String], conditionValues: List[Any], receiver: ActorRef): Unit = {
        //gather answer either from child actors or from associated data
        //TODO check if index exists and use it
        if (children.nonEmpty) {
            receiver ! ExpectResultsMessage(queryID, children.size - 1)
            children.foreach(child => child ! SelectWhereMessage(queryID, projection, conditionColumnNames, conditionOperators, conditionValues, receiver))
        } else {
            val result: ListBuffer[List[Any]] = ListBuffer()
            if (conditionColumnNames.size == 1) {
                val conditionColumn: Int = schema.columnPosition(conditionColumnNames(0))
                if (conditionOperators(0) == "=") {
                    result ++= this.selectWhere(_(conditionColumn) == conditionValues(0))
                } else if (conditionOperators(0) == "<=" && schema.columnDataTypes(conditionColumn) == Int) {
                    result ++= this.selectWhere(_(conditionColumn).asInstanceOf[Int] <= conditionValues(0).asInstanceOf[Int])
                } else if (conditionOperators(0) == ">=" && schema.columnDataTypes(conditionColumn) == Int) {
                    result ++= this.selectWhere(_(conditionColumn).asInstanceOf[Int] >= conditionValues(0).asInstanceOf[Int])
                } else {
                    assert(false, "Operation not supported")
                }
            } else if (conditionColumnNames.size == 2) {
                val conditionColumn1: Int = schema.columnPosition(conditionColumnNames(0))
                if (conditionColumnNames(0) == conditionColumnNames(1) && conditionOperators(0) == "<=" && conditionOperators(1) == ">=" && schema.columnDataTypes(conditionColumn1) == Int) {
                    result ++= this.selectWhere(x => x(conditionColumn1).asInstanceOf[Int] <= conditionValues(0).asInstanceOf[Int] && x(conditionColumn1).asInstanceOf[Int] >= conditionValues(1).asInstanceOf[Int])
                } else {
                    assert(false, "Operation not supported")
                }
            } else {
                assert(false, "Operation not supported")
            }
            if (projection != null) {
                receiver ! QueryResultMessage(queryID, this.projectRows(projection, result.toList))
            } else {
                receiver ! QueryResultMessage(queryID, result.toList)
            }
        }
    }

}
