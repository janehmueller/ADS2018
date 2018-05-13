package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}

import scala.collection.mutable.{Map => MMap}
import de.hpi.ads.remote.messages.QueryResultMessage
import de.hpi.ads.remote.models.Query

object ResultCollectorActor {
    val defaultName = "RESULT_COLLECTOR"

    def props(): Props = Props(new ResultCollectorActor)

    /** Announces a new query with results and where the final results should be sent. */
    case class PrepareNewQueryResultsMessage(queryID: Int, receiver: ActorRef)

    /** Announces that additional results should be expected for a query. */
    case class ExpectResultsMessage(queryID: Int, additionalResults: Int)
}

class ResultCollectorActor extends ADSActor {
    import ResultCollectorActor._
    val queries: MMap[Int, Query] = MMap.empty

    def receive: Receive = {
        /** Create new query object for the query id. */
        case PrepareNewQueryResultsMessage(queryID, receiver) => prepareNewQueryResults(queryID, receiver)

        /** Update the expected number of results for the query. */
        case ExpectResultsMessage(queryID, additionalResults) => expectResults(queryID, additionalResults)

        /** Add result to currently stored results and deliver them if the query is finished. */
        case QueryResultMessage(queryID, result) => queryResult(queryID, result)

        /** Default case */
        case default => log.error(s"Received unknown message: $default")
    }

    def expectResults(queryID: Int, additionalResults: Int): Unit =  {
        queries(queryID).remainingResults += additionalResults
    }

    def prepareNewQueryResults(queryID: Int, receiver: ActorRef): Unit = {
        queries(queryID) = Query(queryID, receiver)
    }

    def queryResult(queryID: Int, rows: List[List[Any]]): Unit = {
        val query = queries(queryID)
        query.addPartialResult(rows)
        if (query.isFinished) {
            query.receiver ! QueryResultMessage(queryID, query.partialResult.toList)
            queries.remove(queryID)
        }
    }

}
