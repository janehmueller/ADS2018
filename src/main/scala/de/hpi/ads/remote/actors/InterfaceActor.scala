package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}

import de.hpi.ads.database.types.TableSchema
import de.hpi.ads.remote.actors.TableActor.{TableInsertRowMessage, TableSelectWhereMessage}
import de.hpi.ads.remote.actors.UserActor.TableCreationSuccessMessage
import de.hpi.ads.remote.messages.QueryFailedMessage

import scala.collection.mutable.{Map => MMap}

object InterfaceActor {
    val defaultName = "INTERFACE"

    var queryCounter: Int = 0

    def nextQueryId: Int = {
        queryCounter += 1
        queryCounter - 1
    }

    def props(): Props = Props(new InterfaceActor)

    case class CreateTableMessage(tableName: String, columnNames: List[String], columnDataTypes: List[Any], columnSizes: List[Int])

    case class InsertRowMessage(tableName: String, data: List[Any])

    case class SelectWhereMessage(tableName: String,
                                  projection: List[String],
                                  conditionColumnNames: List[String],
                                  conditionOperators: List[String],
                                  conditionValues: List[Any])
}

class InterfaceActor extends ADSActor {
    import InterfaceActor._

    val tables: MMap[String, ActorRef] = MMap.empty

    def receive: Receive = {
        case CreateTableMessage(tableName, columnNames, columnDataTypes, columnSizes) => createTable(tableName, columnNames, columnDataTypes, columnSizes)
        case InsertRowMessage(table, data) => insertRow(table, data)
        case SelectWhereMessage(table, projection, conditionColNames, conditionOps, conditionVals) => selectWhere(table, projection, conditionColNames, conditionOps, conditionVals)
        case default => log.error(s"Received unknown message: $default")
    }

    def createTable(tableName: String, columnNames: List[String], columnDataTypes: List[Any], columnSizes: List[Int]): Unit = {
        if (assertTableExistance(tableName, negateCheck = true)) {
            return
        }
        val tableActor = this.context.actorOf(TableActor.props(tableName, fileName(tableName), new TableSchema(columnNames, columnDataTypes, columnSizes)), actorName(tableName))
        tables(tableName) = tableActor
        this.sender() ! TableCreationSuccessMessage(tableName)
    }

    def insertRow(tableName: String, data: List[Any]): Unit = {
        if (assertTableExistance(tableName)) {
            return
        }
        val tableActor = tables(tableName)
        tableActor ! TableInsertRowMessage(data.map(_.toString), this.sender())
    }

    def selectWhere(tableName: String,
                    projection: List[String],
                    conditionColumnNames: List[String],
                    conditionOperators: List[String],
                    conditionValues: List[Any]): Unit = {
        if (assertTableExistance(tableName)) {
            return
        }
        val tableActor = tables(tableName)
        tableActor ! TableSelectWhereMessage(nextQueryId, projection, conditionColumnNames, conditionOperators, conditionValues, this.sender())
    }

    def assertTableExistance(tableName: String, negateCheck: Boolean = false): Boolean = {
        val exists = tables.contains(tableName)
        if (!exists && !negateCheck) {
            this.sender() ! QueryFailedMessage(-1, s"Table $tableName does not exist!")
        } else if (exists && negateCheck) {
            this.sender() ! QueryFailedMessage(-1, s"Table $tableName already exists!")
        }
        !(exists ^ negateCheck)
    }

    def actorName(tableName: String): String = s"TABLE_${tableName.toUpperCase}"

    def fileName(tableName: String): String = s"table.$tableName.ads"
}
