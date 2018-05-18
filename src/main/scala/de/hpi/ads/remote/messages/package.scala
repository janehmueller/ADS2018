/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ads.remote

import akka.actor.ActorRef
import de.hpi.ads.database.Row

package object messages {
    /** Query status messages */
    case class QueryFailureMessage(queryID: Int, message: String)
    case class QueryResultMessage(queryID: Int, result: List[List[Any]])
    case class QuerySuccessMessage(queryID: Int)

    /** Actor state messages */
    case object ShutdownMessage

    /** Table messages */
    /** Table Create */
    case class TableInsertRowMessage(queryID: Int, data: List[Any], receiver: ActorRef)
    case class TableNamedInsertRowMessage(queryID: Int, data: List[(String, Any)], receiver: ActorRef)
    /** Table Read */
    // TODO: conditions that an use an index
    case class TableSelectWhereMessage(queryID: Int, projection: List[String], conditions: Row => Boolean, receiver: ActorRef)
    /** Table Update */
    case class TableUpdateWhereMessage(queryID: Int, data: List[(String, Any)], conditions: Row => Boolean, receiver: ActorRef)
    /** Table Delete */
    case class TableDeleteWhereMessage(queryID: Int, conditions: Row => Boolean, receiver: ActorRef)
}
