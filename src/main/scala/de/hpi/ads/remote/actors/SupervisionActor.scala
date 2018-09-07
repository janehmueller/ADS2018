package de.hpi.ads.remote.actors

import de.hpi.ads.remote.messages._
import akka.actor.{ActorRef, PoisonPill, Props, Terminated}
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

    def receive: Receive = {
        case msg: RequestNewPartitionMessage =>
            this.createNewPartition(msg.props, msg.actorName, msg.receiver, msg.bonusInfo)
    }

    def createNewPartition(props: Props, actorName: String, receiver: ActorRef, bonusInfo: Any): Unit = {
        val ref : ActorRef = context.actorOf(props, actorName)
        receiver ! PartitionCreatedMessage(ref, bonusInfo)
    }
}
