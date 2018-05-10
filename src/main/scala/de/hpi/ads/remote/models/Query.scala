package de.hpi.ads.remote.models

import akka.actor.ActorRef
import de.hpi.ads.database.Row

case class Query(queryID: Int, receiver: ActorRef, var remainingResults: Int, var partialResult: List[Row] = Nil) {

    def addPartialResult(resultRows: List[Row]): Unit = {
        partialResult ++= resultRows
        remainingResults -= 1
    }

    def isFinished: Boolean = remainingResults == 0
}

object Query {
    def apply(queryID: Int, receiver: ActorRef): Query = Query(queryID, receiver, 1)
}
