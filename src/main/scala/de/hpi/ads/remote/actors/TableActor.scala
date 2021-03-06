package de.hpi.ads.remote.actors

import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
import akka.cluster.{Cluster, MemberStatus}
import akka.cluster.ClusterEvent._
import de.hpi.ads.database.types.{ColumnType, TableSchema}

import scala.collection.mutable.{Map => MMap}
import de.hpi.ads.remote.messages._
import de.hpi.ads.remote.actors.TablePartitionActor.fileName

object TableActor {
    def actorName(tableName: String): String = s"TABLE_${tableName.toUpperCase}"

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
    case class TablePartitionReadyMessage(fileName: String, lowerBound: Any, upperBound: Any, actorRef: ActorRef)
    case class InsertionDoneMessage(queryID: Int)
    case class TableExpectDenseInsertRange(lowerBound: Any, upperBound: Any)
    case class ActorReadyMessage(actorRef: ActorRef)
}

class TableActor(tableName: String, schema: TableSchema) extends ADSActor {
    import TableActor._
    import SupervisionActor._

    //possible modes: binary, flat, Bp
    val hierarchyMode: String = context.system.settings.config.getString("ads.hierarchyMode")

    var supervisor: ActorRef = context.actorOf(SupervisionActor.props())
    var firstLevel: Int = 0
    if (hierarchyMode == "Bp") {
        firstLevel = 1
    }
    // TODO: partition file names
    var tablePartitionActor: ActorRef = context.actorOf(
        TablePartitionActor.props(tableName, tableName, schema, this.self, this.supervisor, firstLevel))
    if (hierarchyMode == "Bp") {
        tablePartitionActor ! "setAsTop"
    }
    var currentInsertions: Int = 0
    var currentPartitionings: Int = 0
    var livingDescendants: Int = 0
    var currentlyRebalancing: Boolean = false
    var partitionCollection: MMap[(Any, Any), (String, akka.actor.Address)] = MMap[(Any, Any), (String, akka.actor.Address)]() + ((None, None) -> (fileName(tableName), self.path.address))

    val cluster = Cluster(context.system)
    var clustersToPartitions = MMap[akka.actor.Address, Int]()

    val members = cluster.state.members.filter(_.status == MemberStatus.Up)

    override def postStop(): Unit = {
        super.postStop()
    }

    override def preStart(): Unit = {
        super.preStart()
        cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
            classOf[MemberEvent], classOf[UnreachableMember])
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
            //val queryCollector = this.queryResultCollector(msg.queryID, msg.receiver)
            //msg.receiver = queryCollector
            tablePartitionActor ! msg

        /** Table Update */
        case msg: TableUpdateWhereMessage => tablePartitionActor ! msg

        /** Table Delete */
        case msg: TableDeleteWhereMessage => tablePartitionActor ! msg
        case TablePartitionStartedMessage(fileName, lB, uB, m) => partitionStarted(fileName, lB, uB, m)
        case TablePartitionReadyMessage(fileName, lB, uB, aR) => partitionIsReady(fileName, lB, uB, aR)
        case InsertionDoneMessage(queryID) => {
            currentInsertions -= 1
            if (currentlyRebalancing && currentInsertions + currentPartitionings == 0) {
                rebalance()
            }
        }
        case Terminated(actorRef) => descendantDied(actorRef)
        case msg: TableExpectDenseInsertRange => {
            if (!currentlyRebalancing) {
                tablePartitionActor ! msg
            } else {
                self ! msg
            }
        }
        case "Rebalance" => this.startRebalancing()

        //Cluster management
        case state: CurrentClusterState => {
            /*
            log.info("Wow this is actually a thing!")
            clustersToPartitions = MMap[akka.actor.Address, Int](state.members.collect {
                case m if m.status == akka.cluster.MemberStatus.Up => (m.address, 0)
            }.toMap.toSeq: _*)
            */
        }
        case MemberUp(member) ⇒ {
            log.info("Member is Up: {}", member.address)
            //clustersToPartitions(member) = 1
        }
        case UnreachableMember(member) ⇒
            log.info("Member detected as unreachable: {}", member)
            //we don't deal with this
        case MemberRemoved(member, previousStatus) ⇒
            //we don't deal with this
            log.info(
                "Member is Removed: {} after {}",
                member.address, previousStatus)
        case _: MemberEvent ⇒ // ignore

        /** Handle dropping the table. */
        case ShutdownMessage => {
            if (livingDescendants == 0) {
                context.stop(this.self)
            } else {
                tablePartitionActor ! ShutdownMessage
                tablePartitionActor ! PoisonPill
                self ! ShutdownMessage
            }
        }

        case ActorReadyMessage(actorRef) => {
            context.watch(actorRef)
            livingDescendants += 1
            actorRef.path.address
        }

        /** Default case */
        case default => log.error(s"Received unknown message: $default")
    }

    def queryResultCollector(queryID: Int, receiver: ActorRef): ActorRef = {
        this.context.actorOf(ResultCollectorActor.props(queryID, receiver))
    }

    def partitionStarted(fileName: String, lowerBound: Any, upperBound: Any, middle: Any): Unit = {
        currentPartitionings += 2
        partitionCollection -= ((lowerBound, upperBound))
    }

    def partitionIsReady(fileName: String, lowerBound: Any, upperBound: Any, actorRef: ActorRef): Unit = {
        currentPartitionings -= 1
        partitionCollection += ((lowerBound, upperBound) -> (fileName, actorRef.path.address))
    }

    def descendantDied(actorRef: ActorRef) : Unit = {
        livingDescendants -= 1
        if (currentlyRebalancing && livingDescendants == 0) {
            rebalance()
        }
    }

    def startRebalancing(): Unit = {
        if (hierarchyMode != "binary") {
            //is always balanced
            return
        }
        currentlyRebalancing = true
        tablePartitionActor ! PoisonPill
    }

    def rebalance(): Unit = {
        if (hierarchyMode != "binary") {
            //is always balanced
            return
        }
        // build balanced tree off of partitionCollection
        val dataType = schema.primaryKeyColumn.dataType
        def order = new Ordering[Any] {
            def compare(a: Any, b:Any) = {
                if (a == None) {
                    -1
                } else if (b == None) {
                    1
                } else if (dataType.lessThan(a,b)) {
                    -1
                } else {
                    1
                }
            }
        }
        val sortedSeq = partitionCollection.toSeq.sortBy(_._1._1)(order)
        //if this does not know how to compare Any, use comparison methods that we have elsewhere
        //also maybe we actually dont want a HashMap in the first place, if we have time on insertion but not now
        val treeMap = MMap[(Any, Any), (Boolean, Any)]()
        buildTree(sortedSeq, treeMap, 0, sortedSeq.size)
        log.info(s"Rebalancing sorted sequence: $sortedSeq")
        log.info(s"Rebalancing tree map: $treeMap")
        // build new topPartitionActor who gets entire tree and builds children recursively
        tablePartitionActor = context.actorOf(
            TablePartitionActor.props(tableName, tableName, schema, self, self, None, None, treeMap))
        currentlyRebalancing = false
    }

    def buildTree(sortedSeq: Seq[((Any, Any), (String, akka.actor.Address))], TreeMap: MMap[(Any, Any), (Boolean, Any)], low: Int, high: Int): Unit = {
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
