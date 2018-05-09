package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}
import de.hpi.ads.database.{Row, Table}
import de.hpi.ads.database.types.TableSchema
import de.hpi.ads.remote.actors.UserActor.RowInsertSuccessMessage
import de.hpi.ads.remote.messages.QueryResultMessage

import scala.util.Random

object RowsActor {
    val defaultName = "INTERFACE"

    /**
      * Create Props for an actor of this type.
      *
      * @return a Props for creating this actor, which can then be further configured
      *         (e.g. calling `.withDispatcher()` on it)
      */
    def props(): Props = Props(new InterfaceActor)

    case class InsertRowMessage(data: List[String], receiver: ActorRef)

    case class SelectWhereMessage(queryID: Int, projection: List[String], conditions: Row => Boolean, receiver: ActorRef)

}

class RowsActor(fileName: String, schemaString: String)
    extends Table(fileName, schemaString) with ADSActor {

    import RowsActor._
    import ResultCollectorActor.ExpectResultsMessage

    val children : List[ActorRef] = List()
    val RNG = new Random()


    def receive: Receive = {
        case InsertRowMessage(data, receiver) => insertRow(data, receiver)
        case SelectWhereMessage(queryID, projection, conditions, receiver) => selectWhere(queryID, projection, conditions, receiver)
        case default => log.error(s"Received unknown message: $default")
    }

    def insertRow(data: List[String], receiver: ActorRef): Unit = {
        if (children.nonEmpty) {
            //figure out which child should receive the row according to splitting
            //for now: give it to random child
            children(RNG.nextInt(children.size)) ! InsertRowMessage(data, receiver)
        } else {
            //insert row to self
            this.insertList(data)
            receiver ! RowInsertSuccessMessage
        }
    }

    def selectWhere(queryID: Int, projection: List[String], conditions: Row => Boolean, receiver: ActorRef): Unit = {
        //gather answer either from child actors or from associated data
        if (children.nonEmpty) {
            receiver ! ExpectResultsMessage(queryID, children.size - 1)
            children.foreach(child => child ! SelectWhereMessage(queryID, projection, conditions, receiver))
        } else {
            receiver ! QueryResultMessage(queryID, this.selectWhere(projection, conditions))
        }
    }

}
