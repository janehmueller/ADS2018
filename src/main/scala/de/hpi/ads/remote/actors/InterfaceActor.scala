package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}
import de.hpi.ads.database.Row
import de.hpi.ads.database.types.ColumnType
import de.hpi.ads.remote.actors.UserActor.{TableOpFailureMessage, TableOpSuccessMessage}
import de.hpi.ads.remote.messages._

import scala.collection.mutable.{Map => MMap}

object InterfaceActor {
    val defaultName = "INTERFACE"

    var queryCounter: Int = 0

    def nextQueryId: Int = {
        queryCounter += 1
        queryCounter - 1
    }

    def props(resultCollector: ActorRef): Props = Props(new InterfaceActor(resultCollector))

    /** Table Management */
    case class CreateTableMessage(tableName: String, schema: String)
    case class CreateTableWithTypesMessage(tableName: String, columns: List[ColumnType])
    case class DeleteTableMessage(tableName: String)

    /** Table Create */
    case class InsertRowMessage(tableName: String, data: List[Any])
    case class NamedInsertRowMessage(tableName: String, data: List[(String, Any)])

    // TODO conditions in a format that can use indices
    /** Table Read */
    case class SelectWhereMessage(tableName: String, projection: List[String], conditions: Row => Boolean)

    /** Table Update */
    case class UpdateWhereMessage(tableName: String, data: List[(String, Any)], conditions: Row => Boolean)

    /** Table Delete */
    case class DeleteWhereMessage(tableName: String, conditions: Row => Boolean)
}

class InterfaceActor(resultCollector: ActorRef) extends ADSActor {
    import InterfaceActor._

    val tables: MMap[String, ActorRef] = MMap.empty

    def assertTableExistance(tableName: String, negateCheck: Boolean = false): Boolean = {
        val exists = tables.contains(tableName)
        if (!exists && !negateCheck) {
            this.sender() ! QueryFailureMessage(-1, s"Table $tableName does not exist!")
        } else if (exists && negateCheck) {
            this.sender() ! QueryFailureMessage(-1, s"Table $tableName already exists!")
        }
        !(exists ^ negateCheck)
    }

    def tableExists(tableName: String): Boolean = tables.contains(tableName)

    def receive: Receive = {
        /** Table Management */
        case CreateTableMessage(tableName, schema) => createTable(tableName, schema)
        case CreateTableWithTypesMessage(tableName, columns) => createTable(tableName, columns)
        case DeleteTableMessage(tableName) => deleteTable(tableName)

        /** Table Create */
        case InsertRowMessage(tableName, data) => insertRow(tableName, data)
        case NamedInsertRowMessage(tableName, data) => insertRowWithNames(tableName, data)

        /** Table Read */
        case SelectWhereMessage(tableName, projection, conditions) => selectWhere(tableName, projection, conditions)

        /** Table Update */
        case UpdateWhereMessage(tableName, projection, conditions) => updateWhere(tableName, projection, conditions)

        /** Table Delete */
        case DeleteWhereMessage(tableName, conditions) => deleteWhere(tableName, conditions)

        /** Default case */
        case default => log.error(s"Received unknown message: $default")
    }

    def createTable(tableName: String, schema: String): Unit = {
        if (tableExists(tableName)) {
            this.sender() ! TableOpFailureMessage(tableName, "CREATE", "Table already exists!")
            return
        }
        val tableActor = this.context.actorOf(
            TableActor.props(tableName, schema, resultCollector), TableActor.actorName(tableName)
        )
        tables(tableName) = tableActor
        this.sender ! TableOpSuccessMessage(tableName, "CREATE")
    }

    def createTable(tableName: String, columns: List[ColumnType]): Unit = {
        if (tableExists(tableName)) {
            this.sender() ! TableOpFailureMessage(tableName, "CREATE", "Table already exists!")
            return
        }
        val tableActor = this.context.actorOf(
            TableActor.props(tableName, columns, resultCollector), TableActor.actorName(tableName)
        )
        tables(tableName) = tableActor
        this.sender ! TableOpSuccessMessage(tableName, "CREATE")
    }

    def deleteTable(tableName: String): Unit = {
        if (!tableExists(tableName)) {
            this.sender() ! TableOpFailureMessage(tableName, "DROP", "Table does not exist!")
            return
        }
        val tableActor = tables.remove(tableName).get
        tableActor ! ShutdownMessage
        this.sender ! TableOpSuccessMessage(tableName, "DROP")
    }

    def insertRow(tableName: String, data: List[Any]): Unit = {
        if (!tableExists(tableName)) {
            this.sender() ! TableOpFailureMessage(tableName, "INSERT", "Table does not exist!")
            return
        }
        val tableActor = tables(tableName)
        tableActor ! TableInsertRowMessage(nextQueryId, data, this.sender())
    }

    def insertRowWithNames(tableName: String, data: List[(String, Any)]): Unit = {
        if (!tableExists(tableName)) {
            this.sender() ! TableOpFailureMessage(tableName, "INSERT", "Table does not exist!")
            return
        }
        val tableActor = tables(tableName)
        tableActor ! TableNamedInsertRowMessage(nextQueryId, data, this.sender())
    }

    def selectWhere(tableName: String, projection: List[String], conditions: Row => Boolean): Unit = {
        if (!tableExists(tableName)) {
            this.sender() ! TableOpFailureMessage(tableName, "SELECT", "Table does not exist!")
            return
        }
        val tableActor = tables(tableName)
        tableActor ! TableSelectWhereMessage(nextQueryId, projection, conditions, this.sender())
    }

    def updateWhere(tableName: String, data: List[(String, Any)], conditions: Row => Boolean): Unit = {
        if (!tableExists(tableName)) {
            this.sender() ! TableOpFailureMessage(tableName, "UPDATE", "Table does not exist!")
            return
        }
        val tableActor = tables(tableName)
        tableActor ! TableUpdateWhereMessage(nextQueryId, data, conditions, this.sender())
    }

    def deleteWhere(tableName: String, conditions: Row => Boolean): Unit = {
        if (!tableExists(tableName)) {
            this.sender() ! TableOpFailureMessage(tableName, "DELETE", "Table does not exist!")
            return
        }
        val tableActor = tables(tableName)
        tableActor ! TableDeleteWhereMessage(nextQueryId, conditions, this.sender())
    }
}
