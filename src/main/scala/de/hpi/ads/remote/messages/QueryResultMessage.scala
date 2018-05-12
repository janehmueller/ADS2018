package de.hpi.ads.remote.messages

case class QueryResultMessage(queryID: Int, result: List[List[Any]])
