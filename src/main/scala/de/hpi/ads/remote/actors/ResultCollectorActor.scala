package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}
import de.hpi.ads.database.{Row, Table}
import de.hpi.ads.database.types.TableSchema
import de.hpi.ads.remote.actors.UserActor.RowInsertSuccessMessage
import de.hpi.ads.remote.messages.QueryResultMessage

import scala.collection.mutable.ListBuffer
import scala.util.Random

object ResultCollectorActor {
    val defaultName = "INTERFACE"

    /**
      * Create Props for an actor of this type.
      *
      * @return a Props for creating this actor, which can then be further configured
      *         (e.g. calling `.withDispatcher()` on it)
      */
    def props(): Props = Props(new InterfaceActor)

    case class ExpectResultsMessage(queryID: Int, additionalResults: Int)
    case class PrepareNewQueryResultsMessage(queryID: Int, receiver: ActorRef)

    case class Query(queryID: Int, partialResult: ListBuffer[Row], remainingResults: Int, receiver: ActorRef)
}



class ResultCollectorActor()
    extends ADSActor {

    import ResultCollectorActor._

    var queries : Map[Int, Query] = Map()

    def receive: Receive = {
        case ExpectResultsMessage(queryID, additionalResults) => expectResults(queryID, additionalResults)
        case PrepareNewQueryResultsMessage(queryID, receiver) => prepareNewQueryResults(queryID, receiver)
        case QueryResultMessage(queryID, result) => queryResult(queryID, result)
        case default => log.error(s"Received unknown message: $default")
    }

    def expectResults(queryID: Int, additionalResults: Int) =  {
        queries(queryID).remainingResults += additionalResults
    }

    def prepareNewQueryResults(queryID: Int, receiver: ActorRef) = {
        queries += (queryID -> Query(queryID, ListBuffer(), 1, receiver))
    }

    def queryResult(queryID: Int, rows: List[Row]): Unit = {
        queries(queryID).partialResult ++= rows
        queries(queryID).remainingResults -= 1
        if (queries(queryID).remainingResults == 0) {
            queries(queryID).receiver ! QueryResultMessage(queryID, queries(queryID).partialResult.toList)
        }
    }

}
