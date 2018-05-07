package de.hpi.ads.remote.messages

import de.hpi.ads.database.Row

case class QueryResultMessage(queryID: Int, result: List[Row])
