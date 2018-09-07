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
import de.hpi.ads.database.operators.Operator

package object messages {
    /** Query status messages */
    @SerialVersionUID(110L)
    case class QueryFailureMessage(queryID: Int, message: String) extends Serializable
    @SerialVersionUID(120L)
    case class QueryResultMessage(queryID: Int, result: List[IndexedSeq[Any]]) extends Serializable
    @SerialVersionUID(130L)
    case class QuerySuccessMessage(queryID: Int) extends Serializable

    /** Actor state messages */
    @SerialVersionUID(140L)
    case object ShutdownMessage extends Serializable

    /** Table messages */
    /** Table Create */
    @SerialVersionUID(150L)
    case class TableInsertRowMessage(queryID: Int, data: List[Any], receiver: ActorRef) extends Serializable
    @SerialVersionUID(160L)
    case class TableNamedInsertRowMessage(queryID: Int, data: List[(String, Any)], receiver: ActorRef) extends Serializable
    /** Table Read */
    @SerialVersionUID(170L)
    case class TableSelectWhereMessage(queryID: Int, projection: List[String], operator: Operator, var receiver: ActorRef) extends Serializable
    /** Table Update */
    @SerialVersionUID(180L)
    case class TableUpdateWhereMessage(queryID: Int, data: List[(String, Any)], operator: Operator, receiver: ActorRef) extends Serializable
    /** Table Delete */
    @SerialVersionUID(190L)
    case class TableDeleteWhereMessage(queryID: Int, operator: Operator, receiver: ActorRef) extends Serializable
    @SerialVersionUID(290L)
    case class PartitionCreatedMessage(ref: ActorRef, bonusInfo: Any)
}
