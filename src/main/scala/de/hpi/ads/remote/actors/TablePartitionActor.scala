package de.hpi.ads.remote.actors

import java.nio.file.{Files, Paths}

import akka.actor.{ActorRef, Deploy, PoisonPill, Props}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.remote.RemoteScope
import de.hpi.ads.database.operators.Operator
import de.hpi.ads.database.{Row, Table}
import de.hpi.ads.database.types.TableSchema
import de.hpi.ads.remote.actors.ResultCollectorActor.ExpectResultsMessage
import de.hpi.ads.remote.actors.UserActor.TableOpFailureMessage
import de.hpi.ads.remote.messages._

import scala.collection.SortedSet
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.{Map => MMap}
import scala.util.Random

object TablePartitionActor {
    def path: String = "./tables"

    def actorName(tableName: String): String = s"TABLE_${tableName.toUpperCase}"

    def fileName(tableName: String): String = s"$path/$tableName.table.ads"

    def props(table: String, fileName: String, schema: TableSchema, tableActor: ActorRef): Props = {
        Props(new TablePartitionActor(table, fileName, schema, tableActor, None, None))
    }

    def props(table: String, fileName: String, schema: TableSchema, tableActor: ActorRef, lowerBound: Any, upperBound: Any): Props = {
        Props(new TablePartitionActor(table, fileName, schema, tableActor, lowerBound, upperBound))
    }

    def props(table: String, fileName: String, schema: TableSchema, tableActor: ActorRef, lowerBound: Any, upperBound: Any, data: Array[Byte]): Props = {
        Props(new TablePartitionActor(table, fileName, schema, tableActor, lowerBound, upperBound, data))
    }

    def props(table: String, fileName: String, schema: TableSchema, tableActor: ActorRef, lowerBound: Any, upperBound: Any, hierarchy: MMap[(Any, Any), (Boolean, Any)]): Props = {
        Props(new TablePartitionActor(table, fileName, schema, tableActor, lowerBound, upperBound, hierarchy))
    }

    case class FillWithDataMessage(data: Array[Byte])
}

class TablePartitionActor(tableName: String, fileName: String, schema: TableSchema, tableActor: ActorRef, lowerBound: Any, upperBound: Any)
    extends Table(TablePartitionActor.fileName(fileName), schema) with ADSActor
{
    import TablePartitionActor._
    import TableActor._

    val cluster = Cluster(context.system)
    val children : ListBuffer[ActorRef] = ListBuffer()
    val maxSize: Int = 10000
    var partitionPoint: Any = None
    val members: Seq[Member] = cluster.state.members.filter(_.status == MemberStatus.Up).toSeq
    var nonKeyIndices: MMap[String, MMap[Any, List[Long]]] = MMap.empty
    val RNG = new Random()

    def hasIndex(column: String): Boolean = {
        nonKeyIndices.keySet(column) || this.schema.keyColumn == column
    }

    def this(tableName: String, fileName: String, schema: TableSchema, tableActor: ActorRef, lowerBound: Any, upperBound: Any, initdata: Array[Byte]) = {
        this(tableName, fileName, schema, tableActor, lowerBound, upperBound)
        fillWithData(initdata)
    }

    def this(
        tableName: String,
        fileName: String,
        schema: TableSchema,
        tableActor: ActorRef,
        lowerBound: Any,
        upperBound: Any,
        hierarchy: MMap[(Any, Any), (Boolean, Any)]
    ) = {
        this(tableName, fileName, schema, tableActor, lowerBound, upperBound)
        val entry = hierarchy((lowerBound, upperBound))
        if (entry._1) {
            //nothing to do
        } else {
            //construct child actors
            partitionPoint = entry._2.asInstanceOf[(Any,akka.actor.Address)]._1
            if (hierarchy((lowerBound, partitionPoint))._1) {
                //TODO start at right location
                val leftRangeActor: ActorRef = context.actorOf(TablePartitionActor.props(tableName, hierarchy((lowerBound, partitionPoint))._2.asInstanceOf[String], schema, tableActor, lowerBound, partitionPoint).withDeploy(Deploy(scope = RemoteScope(entry._2.asInstanceOf[(Any,akka.actor.Address)]._2))), hierarchy((lowerBound, partitionPoint))._2.asInstanceOf[String])
                children += leftRangeActor
            } else {
                //TODO start at any location
                val leftRangeActor: ActorRef = context.actorOf(TablePartitionActor.props(tableName, s"${fileName}_$partitionPoint", schema, tableActor, lowerBound, partitionPoint, hierarchy).withDeploy(Deploy(scope = RemoteScope(this.nextMember().address))), s"${fileName}_$partitionPoint")
                children += leftRangeActor
            }
            if (hierarchy((partitionPoint, upperBound))._1) {
                //TODO start at right location
                val rightRangeActor: ActorRef = context.actorOf(TablePartitionActor.props(tableName, hierarchy((partitionPoint, upperBound))._2.asInstanceOf[String], schema, tableActor, partitionPoint, upperBound).withDeploy(Deploy(scope = RemoteScope(entry._2.asInstanceOf[(Any,akka.actor.Address)]._2))), hierarchy((partitionPoint, upperBound))._2.asInstanceOf[String])
                children += rightRangeActor
            } else {
                //TODO start at any location
                val rightRangeActor: ActorRef = context.actorOf(TablePartitionActor.props(tableName, s"${fileName}__$partitionPoint", schema, tableActor, partitionPoint, upperBound, hierarchy).withDeploy(Deploy(scope = RemoteScope(this.nextMember().address))), s"${fileName}__$partitionPoint")
                children += rightRangeActor
            }
        }
    }

    override def preStart(): Unit = {
        super.preStart()
        tableActor ! ActorReadyMessage(self)
    }

    override def postStop(): Unit = {
        super.postStop()
        this.releaseFile()
    }

    def nextMember(): Member = {
        this.members(RNG.nextInt(this.members.size))
    }

    def receive: Receive = {
        /** Table Insert */
        case TableInsertRowMessage(queryID, data, receiver) => insertRow(queryID, data, receiver)
        case TableNamedInsertRowMessage(queryID, data, receiver) => insertRowWithNames(queryID, data, receiver)

        /** Table Read */
        case TableSelectWhereMessage(queryID, projection, operator, receiver) =>
            selectWhere(queryID, projection, operator, receiver)

        /** Table Update */
        case TableUpdateWhereMessage(queryID, data, operator, receiver) =>
            updateWhere(queryID, data, operator, receiver)

        /** Table Delete */
        case TableDeleteWhereMessage(queryID, operator, receiver) =>
            deleteWhere(queryID, operator, receiver)

        case FillWithDataMessage(data) =>
            fillWithData(data)

        case msg: TableExpectDenseInsertRange => expectDenseInsertRange(msg)


        /** Handle dropping the table. */
        case ShutdownMessage => {
            if (children.nonEmpty) {
                children(0) ! ShutdownMessage
                children(1) ! ShutdownMessage
            }
            this.cleanUp()
        }

        /** Default case */
        case default => log.error(s"Received unknown message: $default")
    }

    def insertRow(queryID: Int, data: List[Any], receiver: ActorRef): Unit = {
        if (!inputContainsValidKey(schema.columnNames.toList.zip(data))) {
            receiver ! TableOpFailureMessage(tableName, "INSERT", "Input does not contain valid primary key.")
            tableActor ! InsertionDoneMessage(queryID)
            return
        }
        if (children.nonEmpty) {
            if (schema.primaryKeyColumn.dataType.lessThan(data(this.schema.primaryKeyPosition), partitionPoint)) {
                children(0) ! TableInsertRowMessage(queryID, data, receiver)
            } else {
                children(1) ! TableInsertRowMessage(queryID, data, receiver)
            }
        } else {
            this.insertList(data)
            tableActor ! InsertionDoneMessage(queryID)
            receiver ! QuerySuccessMessage(queryID)
            if (this.keyPositions.size >= this.maxSize) {
                this.actorFull()
            }
        }
    }

    def insertRowWithNames(queryID: Int, data: List[(String, Any)], receiver: ActorRef): Unit = {
        if (!inputContainsValidKey(data)) {
            receiver ! TableOpFailureMessage(tableName, "INSERT", "Input does not contain valid primary key.")
            tableActor ! InsertionDoneMessage(queryID)
            return
        }
        if (children.nonEmpty) {
            if (schema.primaryKeyColumn.dataType.lessThan(data(this.schema.primaryKeyPosition)._2, partitionPoint)) {
                children(0) ! TableInsertRowMessage(queryID, data, receiver)
            } else {
                children(1) ! TableInsertRowMessage(queryID, data, receiver)
            }
        } else {
            this.insert(data)
            tableActor ! InsertionDoneMessage(queryID)
            receiver ! QuerySuccessMessage(queryID)
        }
    }

    /**
      * Validates that the input contains a primary key and that this primary key does not already exist.
      * @param data input data that is validated
      * @return true if the input data is valid
      */
    def inputContainsValidKey(data: List[(String, Any)]): Boolean = {
        val keyExistsOption = data.find(_._1 == schema.keyColumn)
        val duplicateKeyOption = keyExistsOption.filter { case (columnName, key) => this.keyPositions.contains(key) }
        duplicateKeyOption.isEmpty
    }

    def selectWhere(queryID: Int, projection: List[String], operator: Operator, receiver: ActorRef): Unit = {
        // gather answers either from child actors or from associated data
        if (children.nonEmpty) {
            receiver ! ExpectResultsMessage(queryID, children.length - 1)
            children.foreach(_ ! TableSelectWhereMessage(queryID, projection, operator, receiver))
            return
        }
        val tS = System.nanoTime()
        var result: List[Array[Byte]] = Nil
        if (this.hasIndex(operator.column)) {
            var memoryLocations: List[Long] = Nil
            // Use either key index or non-key index
            if (operator.column == this.schema.keyColumn) {
                memoryLocations ++= operator.useKeyIndex(this.keyPositions, schema)
            } else {
                memoryLocations ++= operator(this.nonKeyIndices(operator.column), schema)
            }

            // Only use index with random I/O when we read less than half of this partitions entries
            if (memoryLocations.length < this.keyPositions.size / 2) {
                result = memoryLocations.map(this.readRow)
            } else {
                result = this.selectWhere(row => operator(row, this.schema))
            }
        } else {
            result = this.selectWhere(row => operator(row, this.schema))
        }
        val tE = System.nanoTime()
        log.info(s"Elapsed time (Simple r1): ${(tE - tS)/1000000000.0}s")
        val tS2 = System.nanoTime()
        val projectedResult = result
            .map(Row.fromBytes(_, this.schema))
            .map(Row.project(_, projection.toIndexedSeq, schema))
        val tE2 = System.nanoTime()
        log.info(s"Elapsed time (Simple r2): ${(tE2 - tS2)/1000000000.0}s")
        receiver ! QueryResultMessage(queryID, projectedResult)
    }

    def updateWhere(queryID: Int, data: List[(String, Any)], operator: Operator, receiver: ActorRef): Unit = {
        if (children.nonEmpty) {
            // TODO: figure out which child actors store the relevant rows
        } else {
            // TODO: use index
            this.updateWhere(data, row => operator(row, this.schema))
            receiver ! QuerySuccessMessage(queryID)
        }
    }

    def deleteWhere(queryID: Int, operator: Operator, receiver: ActorRef): Unit = {
        if (children.nonEmpty) {
            // TODO: figure out which child actors store the relevant rows
        } else {
            // TODO: use index
            this.deleteWhere(row => operator(row, this.schema))
            receiver ! QuerySuccessMessage(queryID)
        }
    }

    def actorFull() : Unit = {
        val (half1, half2, keyMedian) = this.readFileHalves
        splitActor(half1, half2, keyMedian)
    }

    def splitActor(half1: Array[Byte], half2: Array[Byte], keyMedian: Any) : Unit = {
        log.info("splitting now")
        tableActor ! TablePartitionStartedMessage(fileName, lowerBound, upperBound, keyMedian)
        val leftRangeActor: ActorRef = context.actorOf(TablePartitionActor.props(tableName, fileName + '0', schema, tableActor, lowerBound, keyMedian, half1).withDeploy(Deploy(scope = RemoteScope(this.nextMember().address))), fileName + '0')
        val rightRangeActor: ActorRef = context.actorOf(TablePartitionActor.props(tableName, fileName + '1', schema, tableActor, keyMedian, upperBound, half2).withDeploy(Deploy(scope = RemoteScope(this.nextMember().address))), fileName + '1')
        children += leftRangeActor
        children += rightRangeActor
        this.partitionPoint = keyMedian
        this.cleanUp()
    }

    def fillWithData(bytes: Array[Byte]): Unit = {
        rebuildTableFromData(bytes)
        tableActor ! TablePartitionReadyMessage(fileName, lowerBound, upperBound, self)
    }

    def expectDenseInsertRange(msg: TableExpectDenseInsertRange): Unit = {
        if (this.children.nonEmpty) {
            if (schema.primaryKeyColumn.dataType.lessThan(msg.lowerBound, partitionPoint)) {
                this.children(0) ! msg
            }
            if (schema.primaryKeyColumn.dataType.lessThan(partitionPoint , msg.upperBound)) {
                this.children(1) ! msg
            }
        } else {
            var min = 0
            if (lowerBound == None) {
                min = msg.lowerBound.asInstanceOf[Int]
            } else {
                min = schema.primaryKeyColumn.dataType.max(msg.lowerBound, lowerBound).asInstanceOf[Int]
            }
            var max = 0
            if (upperBound == None) {
                max = msg.upperBound.asInstanceOf[Int]
            } else {
                max = schema.primaryKeyColumn.dataType.min(msg.upperBound, upperBound).asInstanceOf[Int]
            }
            val additionalRows = max - min //TODO fail for others
            if (additionalRows + this.keyPositions.size >= this.maxSize) {
                val median = schema.primaryKeyColumn.dataType.avg(min, max)
                val (half1, half2) = this.readFileHalves(median)
                splitActor(half1, half2, median)
                children(0) ! msg
                children(1) ! msg
            } else {
            }
        }
    }
}
