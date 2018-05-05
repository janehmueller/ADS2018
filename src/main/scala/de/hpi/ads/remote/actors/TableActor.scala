package de.hpi.ads.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.ads.database.Table

object TableActor {
    def props(fileName: String, schemaString: String): Props = Props(new TableActor(fileName, schemaString))

    case class SelectWhereMessage(
        selectedColumns: List[String],
        conditions: List[(String, Any)],
        queryReceiver: ActorRef
    )

    case class InsertRowMessage(data: List[String], queryReceiver: ActorRef)
}

class TableActor(fileName: String, schemaString: String)
    extends Table(fileName, schemaString) with Actor with ActorLogging
{
    import TableActor._

    def receive: Receive = {
        case SelectWhereMessage => log.info("received select where")
        case InsertRowMessage(data, queryReceiver) =>
            queryReceiver.tell("hello world", this.self)
            log.info("received insert row")
        case _ => log.info("received unknown message")
    }
}


