package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, Props}

import de.hpi.ads.remote.messages.{QueryResultMessage, ShutdownMessage}

object ResultCollectorActor {
    def props(queryID: Int, receiver: ActorRef): Props = Props(new ResultCollectorActor(queryID, receiver))

    /** Announces that additional results should be expected for a query. */
    case class ExpectResultsMessage(queryID: Int, additionalResults: Int)
}

class ResultCollectorActor(queryID: Int, queryReceiver: ActorRef) extends ADSActor {
    import ResultCollectorActor._

    val QueryID: Int = this.queryID

    var remainingResults: Int = 1

    var partialResult: List[List[Any]] = Nil

    def receive: Receive = {
        /** Update the expected number of results for the query. */
        case ExpectResultsMessage(QueryID, additionalResults) => this.remainingResults += additionalResults

        /** Add result to currently stored results and deliver them if the query is finished. */
        case QueryResultMessage(QueryID, result) => queryResult(queryID, result)

        /** After reporting the query result. */
        case ShutdownMessage => context.stop(this.self)

        /** Messages that have a different query id than this result collector. */
        case msg: ExpectResultsMessage => log.error(s"Received message with wrong query id: $msg")
        case msg: QueryResultMessage => log.error(s"Received message with wrong query id: $msg")

        /** Default case */
        case default => log.error(s"Received unknown message: $default")
    }

    def queryResult(queryID: Int, rows: List[List[Any]]): Unit = {
        this.addPartialResult(rows)
        if (this.isFinished) {
            this.queryReceiver ! QueryResultMessage(queryID, this.partialResult)
            this.self ! ShutdownMessage
        }
    }

    def addPartialResult(resultRows: List[List[Any]]): Unit = {
        this.partialResult ++= resultRows
        this.remainingResults -= 1
    }

    def isFinished: Boolean = remainingResults == 0
}
