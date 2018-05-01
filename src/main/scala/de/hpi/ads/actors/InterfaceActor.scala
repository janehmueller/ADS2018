package de.hpi.ads.actors

import akka.actor.Actor
import akka.actor.Props
import akka.event.Logging
import de.hpi.ads.database.types.TableSchema

object InterfaceActor {
    /**
      * Create Props for an actor of this type.
      *
      * @return a Props for creating this actor, which can then be further configured
      *         (e.g. calling `.withDispatcher()` on it)
      */
    def props(): Props = Props(new InterfaceActor())

    case class CreateTableMessage(table: String, schema: TableSchema)

    case class SelectWhereMessage(table: String, selectedColumns: Array[String], conditions: Array[String Tuple2 Any])

    case class InsertRowMessage(table: String, data: Array[Any])
}

class InterfaceActor extends Actor {
    import InterfaceActor._
    val log = Logging(context.system, this)

    def receive = {
        case CreateTableMessage ⇒ log.info("received create table")
        case InsertRowMessage ⇒ log.info("received insert row")
        case SelectWhereMessage ⇒ log.info("received select where")
        case _      ⇒ log.info("received unknown message")
    }
}
