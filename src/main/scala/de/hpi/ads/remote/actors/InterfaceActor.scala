package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}
import de.hpi.ads.database.Row
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

    case class CreateTableMessage(table: String, schema: String)

    case class InsertRowMessage(table: String, data: List[Any])

    case class SelectWhereMessage(table: String, projection: List[String], conditions: Row => Boolean)
}

class InterfaceActor extends ADSActor {
    import InterfaceActor._

    val tables: MMap[String, ActorRef] = MMap.empty

    def receive: Receive = {
        case CreateTableMessage(table, schema) => createTable(table, schema)
        case InsertRowMessage(table, data) => insertRow(table, data)
        case SelectWhereMessage(table, projection, conditions) => selectWhere(table, projection, conditions)
        case default => log.error(s"Received unknown message: $default")
    }

    def createTable(table: String, schema: String): Unit = {
        if (assertTableExistance(table, negateCheck = true)) {
            return
        }
        val tableActor = this.context.actorOf(TableActor.props(table, fileName(table), schema), actorName(table))
        tables(table) = tableActor
        this.sender() ! TableCreationSuccessMessage(table)
    }

    def insertRow(table: String, data: List[Any]): Unit = {
        if (assertTableExistance(table)) {
            return
        }
        val tableActor = tables(table)
        tableActor ! TableInsertRowMessage(data.map(_.toString), this.sender())
    }

    def selectWhere(table: String, projection: List[String], conditions: Row => Boolean): Unit = {
        if (assertTableExistance(table)) {
            return
        }
        val tableActor = tables(table)
        tableActor ! TableSelectWhereMessage(nextQueryId, projection, conditions, this.sender())
    }

    def assertTableExistance(table: String, negateCheck: Boolean = false): Boolean = {
        val exists = tables.contains(table)
        if (!exists && !negateCheck) {
            this.sender() ! QueryFailedMessage(-1, s"Table $table does not exist!")
        } else if (exists && negateCheck) {
            this.sender() ! QueryFailedMessage(-1, s"Table $table already exists!")
        }
        !(exists ^ negateCheck)
    }

    def actorName(table: String): String = s"TABLE_${table.toUpperCase}"

    def fileName(table: String): String = s"table.$table.ads"
}
