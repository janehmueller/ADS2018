package de.hpi.ads.remote.messages

import de.hpi.ads.database.Row

case class QueryResultMessage(result: List[Row])
