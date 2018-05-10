package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}
import de.hpi.ads.database.Row

import scala.collection.mutable.{Map => MMap}
import de.hpi.ads.remote.messages.QueryResultMessage
import de.hpi.ads.remote.models.Query

object ResultCollectorActor {
    def props(): Props = Props(new ResultCollectorActor)

    case class ExpectResultsMessage(queryID: Int, additionalResults: Int)
    case class PrepareNewQueryResultsMessage(queryID: Int, receiver: ActorRef)
}

class ResultCollectorActor extends ADSActor {
    import ResultCollectorActor._

    val queries: MMap[Int, Query] = MMap.empty

    def receive: Receive = {
        case ExpectResultsMessage(queryID, additionalResults) => expectResults(queryID, additionalResults)
        case PrepareNewQueryResultsMessage(queryID, receiver) => prepareNewQueryResults(queryID, receiver)
        case QueryResultMessage(queryID, result) => queryResult(queryID, result)
        case default => log.error(s"Received unknown message: $default")
    }

    def expectResults(queryID: Int, additionalResults: Int): Unit =  {
        queries(queryID).remainingResults += additionalResults
    }

    def prepareNewQueryResults(queryID: Int, receiver: ActorRef): Unit = {
        queries(queryID) = Query(queryID, receiver)
    }

    def queryResult(queryID: Int, rows: List[Row]): Unit = {
        val query = queries(queryID)
        query.addPartialResult(rows)
        if (query.isFinished) {
            query.receiver ! QueryResultMessage(queryID, query.partialResult)
            queries.remove(queryID)
        }
    }

}
