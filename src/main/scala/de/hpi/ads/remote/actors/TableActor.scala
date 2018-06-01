package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, PoisonPill, Props}
import de.hpi.ads.database.types.{ColumnType, TableSchema}

import scala.collection.mutable.{Map => MMap}
import de.hpi.ads.remote.messages._

object TableActor {
    def actorName(tableName: String): String = s"TABLE_${tableName.toUpperCase}"

    def fileName(tableName: String): String = s"$tableName.table.ads"

    def props(table: String, schema: String): Props = {
        Props(new TableActor(table, TableSchema(schema)))
    }

    def props(table: String, columns: IndexedSeq[ColumnType]): Props = {
        Props(new TableActor(table, TableSchema(columns)))
    }

    def props(table: String, columns: TableSchema): Props = {
        Props(new TableActor(table, columns))
    }

    case class TablePartitionStartedMessage(fileName: String, lowerBound: Any, upperBound: Any, middle: Any)
    case class TablePartitionReadyMessage(fileName: String, lowerBound: Any, upperBound: Any)
    case class InsertionDoneMessage(queryID: Int)
}

class TableActor(tableName: String, schema: TableSchema) extends ADSActor {
    import TableActor._

    // TODO: partition file names
    var tablePartitionActor: ActorRef = context.actorOf(
        TablePartitionActor.props(tableName, fileName(tableName), schema, this.self))
    var currentInsertions: Int = 0
    var currentPartitionings: Int = 0
    var currentlyRebalancing: Boolean = false
    var partitionCollection: MMap[(Any, Any), String] = MMap[(Any, Any), String]() + ((None, None) -> fileName(tableName))

    override def postStop(): Unit = {
        super.postStop()
        tablePartitionActor ! PoisonPill
    }

    def receive: Receive = {
        /** Table Insert */
        case msg: TableInsertRowMessage =>
            if (!currentlyRebalancing) {
                tablePartitionActor ! msg
                currentInsertions += 1
            } else {
                self ! msg
            }
        case msg: TableNamedInsertRowMessage =>
            if (!currentlyRebalancing) {
                tablePartitionActor ! msg
                currentInsertions += 1
            } else {
                self ! msg
            }

        /** Table Read */
        case msg: TableSelectWhereMessage =>
            val queryCollector = this.queryResultCollector(msg.queryID, msg.receiver)
            msg.receiver = queryCollector
            tablePartitionActor ! msg

        /** Table Update */
        case msg: TableUpdateWhereMessage => tablePartitionActor ! msg

        /** Table Delete */
        case msg: TableDeleteWhereMessage => tablePartitionActor ! msg
        case TablePartitionStartedMessage(fileName, lB, uB, m) => partitionStarted(fileName, lB, uB, m)
        case TablePartitionReadyMessage(fileName, lB, uB) => partitionIsReady(fileName, lB, uB)
        case InsertionDoneMessage(queryID) =>
            currentInsertions -= 1
            if (currentlyRebalancing && currentInsertions + currentPartitionings == 0) {
                rebalance()
            }

        /** Handle dropping the table. */
        case ShutdownMessage =>
            tablePartitionActor ! ShutdownMessage
            context.stop(this.self)

        /** Default case */
        case default => log.error(s"Received unknown message: $default")
    }

    def queryResultCollector(queryID: Int, receiver: ActorRef): ActorRef = {
        this.context.actorOf(ResultCollectorActor.props(queryID, receiver))
    }

    def partitionStarted(fileName: String, lowerBound: Any, upperBound: Any, middle: Any): Unit = {
        currentPartitionings += 2
        partitionCollection -= ((lowerBound, upperBound) -> fileName)
    }

    def partitionIsReady(fileName: String, lowerBound: Any, upperBound: Any): Unit = {
        currentPartitionings -= 1
        partitionCollection += ((lowerBound, upperBound) -> fileName)
    }

    def startRebalancing(): Unit = {
        currentlyRebalancing = true
        if (currentInsertions + currentPartitionings == 0) {
            rebalance()
        }
    }

    def rebalance(): Unit = {
        tablePartitionActor ! PoisonPill
        // build balanced tree off of partitionCollection
        val sortedSeq = partitionCollection.toSeq.sortBy(_._1):_* // TODO: specify implicit ordering
        //if this does not know how to compare Any, use comparison methods that we have elsewhere
        //also maybe we actually dont want a HashMap in the first place, if we have time on insertion but not now
        val treeMap = MMap[(Any, Any), (Boolean, Any)]()
        buildTree(sortedSeq, treeMap, 0, sortedSeq.size)
        // build new topPartitionActor who gets entire tree and builds children recursively
        tablePartitionActor = context.actorOf(
            TablePartitionActor.props(tableName, fileName(tableName), schema, self, None, None, treeMap))
        currentlyRebalancing = false
    }

    def buildTree(sortedSeq: Seq[((Any, Any), String)], TreeMap: MMap[(Any, Any), (Boolean, Any)], low: Int, high: Int): Unit = {
        if (low >= high-1) {
            TreeMap += (sortedSeq(low)._1._1, sortedSeq(low)._1._2) -> (true, sortedSeq(low)._2)
        } else {
            //high is at least low+2, so we need to take care of >=2 entries, so split
            TreeMap += (sortedSeq(low)._1._1, sortedSeq(high-1)._1._2) -> (false, sortedSeq((low+high)/2)._1._1)
            buildTree(sortedSeq, TreeMap, low, (low+high)/2)
            buildTree(sortedSeq, TreeMap, (low+high)/2, high)
        }
    }
}
