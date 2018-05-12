package de.hpi.ads.remote.models

import akka.actor.ActorRef

import scala.collection.mutable.ListBuffer

case class Query(queryID: Int, receiver: ActorRef, var remainingResults: Int, var partialResult: ListBuffer[List[Any]] = new ListBuffer[List[Any]]) {

    def addPartialResult(resultRows: List[List[Any]]): Unit = {
        partialResult ++= resultRows
        remainingResults -= 1
    }

    def isFinished: Boolean = remainingResults == 0
}

object Query {
    def apply(queryID: Int, receiver: ActorRef): Query = Query(queryID, receiver, 1)
}
