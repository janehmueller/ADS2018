package de.hpi.ads.remote.actors

import de.hpi.ads.remote.messages._
import akka.actor.{ActorRef, Deploy, PoisonPill, Props, Terminated}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.remote.RemoteScope

import scala.util.Random
object SupervisionActor {

    def props(): Props = {
        Props(new SupervisionActor())
    }

    @SerialVersionUID(280L)
    case class RequestNewPartitionMessage(props: Props, actorName: String, receiver: ActorRef, bonusInfo: Any)

}

class SupervisionActor() extends ADSActor
{
    import SupervisionActor._

    val cluster = Cluster(context.system)
    val members: Seq[Member] = cluster.state.members.filter(_.status == MemberStatus.Up).toSeq
    val RNG = new Random()

    def nextMember(): Member = {
        this.members(RNG.nextInt(this.members.size))
    }

    def receive: Receive = {
        case msg: RequestNewPartitionMessage =>
            this.createNewPartition(msg.props, msg.actorName, msg.receiver, msg.bonusInfo)
    }

    def createNewPartition(props: Props, actorName: String, receiver: ActorRef, bonusInfo: Any): Unit = {
        val ref : ActorRef = context.actorOf(props.withDeploy(Deploy(scope = RemoteScope(this.nextMember().address))), actorName)
        receiver ! PartitionCreatedMessage(ref, bonusInfo)
    }
}
