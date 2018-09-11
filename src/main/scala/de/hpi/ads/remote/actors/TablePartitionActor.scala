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

    def props(table: String, fileName: String, schema: TableSchema, tableActor: ActorRef, supervisor: ActorRef): Props = {
        Props(new TablePartitionActor(table, fileName, schema, tableActor, supervisor, None, None))
    }

    def props(table: String, fileName: String, schema: TableSchema, tableActor: ActorRef, supervisor: ActorRef, level: Int): Props = {
        Props(new TablePartitionActor(table, fileName, schema, tableActor, supervisor, None, None, level = level))
    }

    def props(table: String, fileName: String, schema: TableSchema, tableActor: ActorRef, supervisor: ActorRef, lowerBound: Any, upperBound: Any): Props = {
        Props(new TablePartitionActor(table, fileName, schema, tableActor, supervisor, lowerBound, upperBound))
    }

    def props(table: String, fileName: String, schema: TableSchema, tableActor: ActorRef, supervisor: ActorRef, lowerBound: Any, upperBound: Any, data: Array[Byte]): Props = {
        Props(new TablePartitionActor(table, fileName, schema, tableActor, supervisor, lowerBound, upperBound, data))
    }

    def props(table: String, fileName: String, schema: TableSchema, tableActor: ActorRef, supervisor: ActorRef, lowerBound: Any, upperBound: Any, hierarchy: MMap[(Any, Any), (Boolean, Any)]): Props = {
        Props(new TablePartitionActor(table, fileName, schema, tableActor, supervisor, lowerBound, upperBound, hierarchy))
    }

    case class FillWithDataMessage(data: Array[Byte])
    case class ChildrenAreFullMessage(maxVal: Any)

}

class TablePartitionActor(tableName: String, fileName: String, schema: TableSchema, tableActor: ActorRef, var supervisor: ActorRef, lowerBound: Any, upperBound: Any, level: Int = -1)
    extends Table(TablePartitionActor.fileName(fileName), schema) with ADSActor
{
    import TablePartitionActor._
    import TableActor._
    import SupervisionActor._
    import de.hpi.ads.remote.messages._

    //possible modes: binary, flat, Bp
    val hierarchyMode: String = context.system.settings.config.getString("ads.hierarchyMode")
    val maxChildren: Int = context.system.settings.config.getInt("ads.maxChildren")

    val cluster = Cluster(context.system)
    val children : ListBuffer[ActorRef] = ListBuffer()
    var currentlySplitting : Boolean = false
    val maxSize: Int = 10
    var partitionPoint: Any = None
    val partitionPoints: ListBuffer[Any] = ListBuffer()
    val members: Seq[Member] = cluster.state.members.filter(_.status == MemberStatus.Up).toSeq
    var nonKeyIndices: MMap[String, MMap[Any, List[Long]]] = MMap.empty
    val RNG = new Random()
    var isTop = false
    var isFullMessageSent = false
    var reportedMax: Any = None
    log.warning("Starting actor at level " + level.toString + " with filename " + fileName)
    if (level > 0) {
        val ref = context.actorOf(TablePartitionActor.props(tableName, fileName + 'f', schema, tableActor, self, level - 1).withDeploy(Deploy(scope = RemoteScope(this.nextMember().address))))
        children += ref
    }

    def hasIndex(column: String): Boolean = {
        nonKeyIndices.keySet(column) || this.schema.keyColumn == column
    }

    def this(tableName: String, fileName: String, schema: TableSchema, tableActor: ActorRef, supervisor: ActorRef, lowerBound: Any, upperBound: Any, initdata: Array[Byte]) = {
        this(tableName, fileName, schema, tableActor, supervisor, lowerBound, upperBound)
        fillWithData(initdata)
    }

    def this(
        tableName: String,
        fileName: String,
        schema: TableSchema,
        tableActor: ActorRef,
        supervisor: ActorRef,
        lowerBound: Any,
        upperBound: Any,
        hierarchy: MMap[(Any, Any), (Boolean, Any)]
    ) = {
        this(tableName, fileName, schema, tableActor, supervisor, lowerBound, upperBound)
        val entry = hierarchy((lowerBound, upperBound))
        if (entry._1) {
            //nothing to do
        } else {
            //construct child actors
            partitionPoint = entry._2
            if (hierarchy((lowerBound, partitionPoint))._1) {
                val leftRangeActor: ActorRef = context.actorOf(TablePartitionActor.props(tableName, hierarchy((lowerBound, partitionPoint))._2.asInstanceOf[(String,akka.actor.Address)]._1, schema, tableActor, self, lowerBound, partitionPoint).withDeploy(Deploy(scope = RemoteScope(hierarchy((lowerBound, partitionPoint))._2.asInstanceOf[(Any,akka.actor.Address)]._2))), hierarchy((lowerBound, partitionPoint))._2.asInstanceOf[(String,akka.actor.Address)]._1)
                children += leftRangeActor
            } else {
                val leftRangeActor: ActorRef = context.actorOf(TablePartitionActor.props(tableName, s"${fileName}_$partitionPoint", schema, tableActor, self, lowerBound, partitionPoint, hierarchy).withDeploy(Deploy(scope = RemoteScope(this.nextMember().address))), s"${fileName}_$partitionPoint")
                children += leftRangeActor
            }
            if (hierarchy((partitionPoint, upperBound))._1) {
                val rightRangeActor: ActorRef = context.actorOf(TablePartitionActor.props(tableName, hierarchy((partitionPoint, upperBound))._2.asInstanceOf[(String,akka.actor.Address)]._1, schema, tableActor, self, partitionPoint, upperBound).withDeploy(Deploy(scope = RemoteScope(hierarchy((lowerBound, partitionPoint))._2.asInstanceOf[(Any,akka.actor.Address)]._2))), hierarchy((partitionPoint, upperBound))._2.asInstanceOf[(String,akka.actor.Address)]._1)
                children += rightRangeActor
            } else {
                val rightRangeActor: ActorRef = context.actorOf(TablePartitionActor.props(tableName, s"${fileName}__$partitionPoint", schema, tableActor, self, partitionPoint, upperBound, hierarchy).withDeploy(Deploy(scope = RemoteScope(this.nextMember().address))), s"${fileName}__$partitionPoint")
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
        case msg: TableInsertRowMessage => {
            //log.warning(s"Received message: $msg")
            if (this.currentlySplitting) {
                self ! msg
            } else {
                insertRow(msg.queryID, msg.data, msg.receiver)
            }
        }

        case msg: TableNamedInsertRowMessage => {
            if (this.currentlySplitting) {
                self ! msg
            } else {
                insertRowWithNames(msg.queryID, msg.data, msg.receiver)
            }
        }

        /** Table Read */
        case msg: TableSelectWhereMessage => {
            if (this.currentlySplitting) {
                self ! msg
            } else {
                selectWhere(msg.queryID, msg.projection, msg.operator, msg.receiver)
            }
        }

        /** Table Update */
        case msg: TableUpdateWhereMessage => {
            if (this.currentlySplitting) {
                self ! msg
            } else {
                updateWhere(msg.queryID, msg.data, msg.operator, msg.receiver)
            }
        }

        /** Table Delete */
        case msg: TableDeleteWhereMessage => {
            if (this.currentlySplitting) {
                self ! msg
            } else {
                deleteWhere(msg.queryID, msg.operator, msg.receiver)
            }
        }

        case msg: FillWithDataMessage => {
            //log.warning(s"Received message: $msg")
            if (this.currentlySplitting) {
                self ! msg
            } else {
                fillWithData(msg.data)
            }
        }

        case msg: TableExpectDenseInsertRange => {
            if (this.currentlySplitting) {
                self ! msg
            } else {
                expectDenseInsertRange(msg)
            }
        }

        case msg: PartitionCreatedMessage => {
            assert(this.hierarchyMode == "flat")
            if (msg.bonusInfo.asInstanceOf[Boolean] == true) {
                //left child
                this.children prepend msg.ref
            } else if (msg.bonusInfo.asInstanceOf[Boolean] == false) {
                //right child
                this.children += msg.ref
            }
            if (this.children.size == 2) {
                this.currentlySplitting = false
            }
        }

        case "setAsTop" => {
            //log.warning(s"Received message: setastop")
            this.isTop = true
        }

        case msg: ChildrenAreFullMessage => {
            assert(this.hierarchyMode == "Bp")
            //log.warning(s"Received message: $msg")
            if (this.children.size == maxChildren) {
                if (!this.isTop) {
                    //escalate
                    //log.warning("Got children are full, reacting with escalation")
                    supervisor ! msg
                } else {
                    //start up new top
                    //log.warning("Got children are full, reacting with starting new top")
                    supervisor = context.actorOf(TablePartitionActor.props(tableName, fileName + 'f', schema, tableActor, self, level + 1).withDeploy(Deploy(scope = RemoteScope(this.nextMember().address))))
                    this.isTop = false
                    supervisor ! "setAsTop"
                    children += supervisor
                    partitionPoints += msg.maxVal
                }
            } else {
                //log.warning("Got children are full, reacting with starting new child")
                this.partitionPoints += msg.maxVal
                //start new child
                val ref = context.actorOf(TablePartitionActor.props(tableName, fileName + (children.size + 1).toString, schema, tableActor, self, level - 1).withDeploy(Deploy(scope = RemoteScope(this.nextMember().address))))
                children += ref
            }
        }


        /** Handle dropping the table. */
        case ShutdownMessage => {
            if (this.currentlySplitting) {
                self ! ShutdownMessage
            } else {
                if (children.nonEmpty) {
                    children(0) ! ShutdownMessage
                    children(1) ! ShutdownMessage
                }
                this.cleanUp()
            }
        }

        /** Default case */
        case default => log.error(s"Received unknown message: $default")
    }

    def insertRow(queryID: Int, data: List[Any], receiver: ActorRef): Unit = {
        //log.warning(s"Inserting row " + queryID.toString)
        if (!inputContainsValidKey(schema.columnNames.toList.zip(data))) {
            receiver ! TableOpFailureMessage(tableName, "INSERT", "Input does not contain valid primary key.")
            tableActor ! InsertionDoneMessage(queryID)
            return
        }
        if (hierarchyMode == "Bp") {
            if (level <= 0) {
                if (isFullMessageSent && schema.primaryKeyColumn.dataType.lessThan(reportedMax, data(this.schema.primaryKeyPosition))) {
                    //return to top
                    supervisor ! TableInsertRowMessage(queryID, data, receiver)
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
                        if (!isFullMessageSent) {
                            reportedMax = getPrimaryKeyMax
                            supervisor ! ChildrenAreFullMessage(reportedMax)
                            isFullMessageSent = true
                        }
                    }
                }
            } else {
                //level > 0
                val primKey = data(this.schema.primaryKeyPosition)
                for (i <- 0 until partitionPoints.length) {
                    if (schema.primaryKeyColumn.dataType.lessThan(primKey, partitionPoints(i))) {
                        children(i) ! TableInsertRowMessage(queryID, data, receiver)
                        return
                    }
                }
                children.last ! TableInsertRowMessage(queryID, data, receiver)
            }
        } else {
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

    }

    def insertRowWithNames(queryID: Int, data: List[(String, Any)], receiver: ActorRef): Unit = {
        assert(hierarchyMode != "Bp", "B+ tree hierarchy not yet fully supported!")
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
        if (this.hierarchyMode == "binary" || this.hierarchyMode == "Bp") {
            tableActor ! TablePartitionStartedMessage(fileName, lowerBound, upperBound, keyMedian)
            val leftRangeActor: ActorRef = context.actorOf(TablePartitionActor.props(tableName, fileName + '0', schema, tableActor, self, lowerBound, keyMedian, half1).withDeploy(Deploy(scope = RemoteScope(this.nextMember().address))), fileName + '0')
            val rightRangeActor: ActorRef = context.actorOf(TablePartitionActor.props(tableName, fileName + '1', schema, tableActor, self, keyMedian, upperBound, half2).withDeploy(Deploy(scope = RemoteScope(this.nextMember().address))), fileName + '1')
            children += leftRangeActor
            children += rightRangeActor
            this.partitionPoint = keyMedian
            this.cleanUp()
        } else if (this.hierarchyMode == "flat") {
            this.currentlySplitting = true
            tableActor ! TablePartitionStartedMessage(fileName, lowerBound, upperBound, keyMedian)

            //same supervisor for now
            log.warning("Splitting and sending")
            this.supervisor ! RequestNewPartitionMessage(TablePartitionActor.props(tableName, fileName + '0', schema, tableActor, this.supervisor, lowerBound, keyMedian, half1), fileName + '0', self, true)
            this.supervisor ! RequestNewPartitionMessage(TablePartitionActor.props(tableName, fileName + '1', schema, tableActor, this.supervisor, keyMedian, upperBound, half2), fileName + '1', self, false)

            this.partitionPoint = keyMedian

            // hoping that the messages succeed, otherwise this is too early and we loose the data
            this.cleanUp()
        } else {
            assert(false)
        }
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
