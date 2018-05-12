package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}
import de.hpi.ads.database.types.TableSchema

object TableActor {
    def props(table: String, fileName: String, schema: TableSchema): Props = {
        Props(new TableActor(table, fileName, schema))
    }

    case class TableSelectWhereMessage(
         queryID: Int,
         projection: List[String],
         conditionColumnNames: List[String],
         conditionOperators: List[String],
         conditionValues: List[Any],
         receiver: ActorRef
    )

    case class TableInsertRowMessage(data: List[Any], queryReceiver: ActorRef)
}

class TableActor(table: String, fileName: String, schema: TableSchema)
    extends ADSActor
{
    import TableActor._
    val rowActor = context.actorOf(RowsActor.props("fileWhatever", schema), name = "topRowsActor") //TODO find unique filenames
    val resultCollectorActor = context.actorOf(ResultCollectorActor.props(), name="tableResultCollector")
    def receive: Receive = {
        case msg: TableSelectWhereMessage =>
            resultCollectorActor ! ResultCollectorActor.PrepareNewQueryResultsMessage(msg.queryID, msg.receiver)
            rowActor ! RowsActor.SelectWhereMessage(msg.queryID, msg.projection, msg.conditionColumnNames, msg.conditionOperators, msg.conditionValues, resultCollectorActor)
        case TableInsertRowMessage(data, queryReceiver) =>
            rowActor ! RowsActor.InsertRowMessage(data, queryReceiver)
        case default => log.error(s"Received unknown message: $default")
    }
}


